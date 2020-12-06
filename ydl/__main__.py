
import argparse
import datetime
import json
import os
import re
import sqlite3
import sys
import time
import traceback
import urllib
import ydl

import logging
logging.basicConfig(level=logging.ERROR)

from sqlitehelper import SH, DBTable, DBCol

def sec_str(sec):
	min,sec = divmod(sec, 60)
	hr,min = divmod(min, 60)

	if hr > 0:
		return "%d:%02d:%02d" % (hr,min,sec)
	elif min > 0:
		return "%d:%02d" % (min,sec)
	else:
		return "0:%d" % sec

def _now():
	""" Now """
	return datetime.datetime.utcnow()

def inputopts(txt):
	opts = re.findall("\([a-zA-Z0-9]+\)", txt)
	opts = [_[1:-1] for _ in opts]

	default = [_ for _ in opts if _.isupper()]
	if len(default):
		default = default[0]
	else:
		default = None

	opts = [_.lower() for _ in opts]

	while True:
		ret = input(txt)

		if not len(ret):
			if default:
				return default
			else:
				continue
		elif ret.lower() in opts:
			return ret.lower()
		else:
			print("Option '%s' not recognized, try again" % ret)
			continue

class db(SH):
	__schema__ = [
		DBTable('v',
			DBCol('ytid', 'text'),
			DBCol('name', 'text'),  # File name of the saved video
			DBCol('dname', 'text'), # Directory the file will be saved in (based on which list it is added from first)
			DBCol('duration', 'integer'),
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ptime', 'datetime'), # Upload time to youtube (whatever they say it is)
			DBCol('ctime', 'datetime'), # Creation time (first time this video was put in the list)
			DBCol('atime', 'datetime'), # Access time (last time this video was touched)
			DBCol('utime', 'datetime'),  # Update time (last time anything for the video was downloaded)

			# Put long strings at the end
			DBCol('thumbnails', 'json')
		),
		DBTable('chapters',
			DBCol('ytid', 'text'),
			DBCol('dat', 'json'),
		),
		DBTable('mergers',
			DBCol('ytid', 'text'),
			DBCol('dat', 'json'),
		),
		# Playlists
		DBTable('pl',
			DBCol('ytid', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Named channels
		DBTable('c',
			DBCol('name', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Unnamed channels
		DBTable('ch',
			DBCol('name', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Users
		DBTable('u',
			DBCol('name', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		DBTable('vids',
			DBCol('name', 'text'),
			DBCol('ytid', 'text'),
			DBCol('idx', 'integer'),
			DBCol('atime', 'datetime'),
		),
	]
	def __init__(self, fname):
		super().__init__(fname)

	def open(self):
		ex = os.path.exists(self.Filename)

		super().open()

		if not ex:
			self.MakeDatabaseSchema()


	def get_user(self, name):
		return self.u.select_one("*", "name=?", [name])

	def get_playlist(self, ytid):
		return self.pl.select_one("*", "ytid=?", [ytid])

	def get_channel_named(self, name):
		return self.c.select_one("*", "name=?", [name])

	def get_channel_unnamed(self, name):
		return self.ch.select_one("*", "name=?", [name])


	def add_user(self, name):
		return self.u.insert(name=name, ctime=_now())

	def add_playlist(self, ytid):
		return self.pl.insert(ytid=ytid, ctime=_now())

	def add_channel_named(self, name):
		return self.c.insert(name=name, ctime=_now())

	def add_channel_unnamed(self, name):
		return self.ch.insert(name=name, ctime=_now())

def sync_channels_named(d, ignore_old):
	"""
	Sync "named" channels (I don't know how else to call them) that are /c/NAME
	as opposed to "unnamed" channels that are at /channel/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.
	"""

	# Filter based on atime being null if @ignore_old is True
	where = ""
	if ignore_old:
		where = "`atime` is null"

	# Get lists
	res = d.c.select(['rowid',"name"], where)

	# Update atimes
	d.begin()
	rows = [dict(_) for _ in res]
	for row in rows:
		d.c.update({'rowid': row['rowid']}, {'atime': _now()})
	d.commit()

	# Prune it down to just the names and sort
	rows = [_['name'] for _ in rows]
	rows = sorted(rows)

	# Sync the lists
	_sync_list(d, rows, ydl.get_list_c)

def sync_users(d, ignore_old):
	"""
	Sync user videos

	Use the database object @d to sync users.
	If @ignore_old is True then skip those that have been sync'ed before.
	"""

	# Filter based on atime being null if @ignore_old is True
	where = ""
	if ignore_old:
		where = "`atime` is null"

	res = d.u.select(['rowid',"name"], where)

	# Update atimes
	d.begin()
	rows = [dict(_) for _ in res]
	for row in rows:
		d.u.update({'rowid': row['rowid']}, {'atime': _now()})
	d.commit()

	# Prune it down to just the names and sort
	rows = [_['name'] for _ in rows]
	rows = sorted(rows)

	# Sync the lists
	_sync_list(d, rows, ydl.get_list_user)

def sync_channels_unnamed(d, ignore_old):
	"""
	Sync "unnamed" channels (I don't know how else to call them) that are /channel/NAME
	as opposed to "named" channels that are at /c/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.
	"""

	# Filter based on atime being null if @ignore_old is True
	where = ""
	if ignore_old:
		where = "`atime` is null"

	res = d.ch.select(['rowid',"name"], where)

	# Update atimes
	d.begin()
	rows = [dict(_) for _ in res]
	for row in rows:
		d.ch.update({'rowid': row['rowid']}, {'atime': _now()})
	d.commit()

	# Prune it down to just the names and sort
	rows = [_['name'] for _ in rows]
	rows = sorted(rows)

	# Sync the lists
	_sync_list(d, rows, ydl.get_list_channel)

def sync_playlists(d, ignore_old):
	"""
	Sync all playlists.

	Use the database object @d to sync all playlists.
	If @ignore_old is True then skip those that have been sync'ed before.
	"""

	# Filter based on atime being null if @ignore_old is True
	where = ""
	if ignore_old:
		where = "`atime` is null"

	res = d.pl.select(['rowid',"ytid"], where)

	# Update atimes
	d.begin()
	rows = [dict(_) for _ in res]
	for row in rows:
		d.pl.update({'rowid': row['rowid']}, {'atime': _now()})
	d.commit()

	# Prune it down to just the names and sort
	rows = [_['ytid'] for _ in rows]
	rows = sorted(rows)

	# Sync the lists
	_sync_list(d, rows, ydl.get_list_playlist)

def _sync_list(d, names, f_get_list):
	"""
	Base function that does all the list syncing.

	@d is the database object
	@names is a simple array of names to print out and reference `vids` entries to
	@f_get_list is a function in ydl library that gets videos for the given list (as this is unique for each list type, it must be supplied
	"""

	for c_name in names:
		d.begin()

		# Print the name out to show progress
		print("\t%s" % c_name)

		try:
			cur = f_get_list(c_name, getVideoInfo=False)
			cur = cur[0]

			# Index old values by ytid to the rowid for updating
			res = d.vids.select(["rowid","ytid"], "name=?", [c_name])
			old = {r['ytid']:r['rowid'] for r in res}

			# Update or add video to list in vids table
			for v in cur['info']:
				# Update old index
				if v['ytid'] in old:
					print("\t\t%d: %s (OLD)" % (v['idx'], v['ytid']))
					d.vids.update({'rowid': old[v['ytid']]}, {'idx': v['idx'], 'atime': _now()})

					# Remove from the old list (anything not removed will be considered deleted from the list)
					del old[v['ytid']]
				else:
					print("\t\t%d: %s (NEW)" % (v['idx'], v['ytid']))
					d.vids.insert(name=c_name, ytid=v['ytid'], idx=v['idx'], atime=_now())

			# Delete all old entries that are no longer on the list
			for ytid,rowid in old.items():
				d.vids.delete(rowid)

			# Update or add video to the global videos list
			for v in cur['info']:
				vrow = d.v.select_one("rowid", "ytid=?", [v['ytid']])
				if vrow:
					d.v.update({'rowid': vrow['rowid']}, {'atime': None}) 
				else:
					n = _now()
					# FIXME: dname is whatever list adds it first, but should favor
					# the channel. Can happen if a playlist is added first, then the channel
					# the video is on is added later.
					d.v.insert(ytid=v['ytid'], ctime=n, atime=None, dname=c_name)

		except Exception:
			traceback.print_exc()
			# Continue onward, ignore errors

		d.commit()


def sync_videos(d, ignore_old):
	"""
	Sync all videos in the database @d and if @ignore_old is True then don't sync
	those videos that have been sync'ed before.
	"""

	if ignore_old:
		res = d.v.select(['rowid','ytid','ctime'], "utime is null")
	else:
		res = d.v.select(['rowid','ytid','ctime'], "")

	# Convert rows to dictionaries
	rows = [dict(_) for _ in res]
	# Sort by YTID to be consistent
	rows = sorted(rows, key=lambda x: x['ytid'])

	# Iterate over videos
	for i,row in enumerate(rows):
		ytid = row['ytid']
		rowid = row['rowid']
		ctime = row['ctime']

		# print to the screen to show progress
		print("\t%d of %d: %s" % (i+1,len(rows), ytid))

		# Get video information
		ret = ydl.get_info_video(ytid)

		# Squash non-ASCII characters (I don't like emoji in file names)
		name = ret['title'].encode('ascii', errors='ignore').decode('ascii')

		# Format
		atime = _now()
		if ctime is None:
			ctime = atim

		# Aggregate data
		dat = {
			'ytid': ytid,
			'duration': ret['duration'],
			'title': ret['title'],
			'name': name,
			'uploader': ret['uploader'],
			'thumbnails': json.dumps(ret['thumbnails']),
			'ptime': datetime.datetime.strptime(ret['upload_date'], "%Y%m%d"),
			'ctime': ctime,
			'atime': atime,
			'utime': atime,
		}

		# Do actual update
		d.begin()
		d.v.update({'rowid': rowid}, dat)
		d.commit()

def download_videos(d, ignore_old):
	res = d.v.select(['rowid'], "")
	total = len(res.fetchall())

	print("%d videos in database" % total)

	if ignore_old:
		res = d.v.select(['rowid','ytid','title','name','dname'], "`utime` is null")
		rows = res.fetchall()
		print("Ignoring old videos, %d left" % len(rows))
	else:
		res = d.v.select(['rowid','ytid','title','name','dname'], "")
		rows = res.fetchall()

	# Convert to dictionaries and index by ytid
	rows = [dict(_) for _ in rows]
	rows = {_['ytid']:_ for _ in rows}

	# Sort by ytids
	ytids = list(rows.keys())
	ytids = sorted(ytids)

	for i,ytid in enumerate(ytids):
		row = rows[ytid]
		print("\t%d of %d: %s" % (i+1, len(rows), row['ytid']))

		cwd = os.getcwd()
		dname = os.path.join(cwd, row['dname'])

		# Make subdir if it doesn't exist
		if not os.path.exists(dname):
			os.mkdir(dname)

		try:
			ydl.download(row['ytid'], row['name'], dname)
		except KeyboardInterrupt:
			break
		except:
			# Print it out to see it
			traceback.print_exc()
			# Skip errors, and keep downloading
			continue

		d.begin()
		d.v.update({"rowid": row['rowid']}, {'name': name, 'utime': _now()})
		d.commit()


def _main():
	""" Main function called from invoking the library """

	p = argparse.ArgumentParser()
	p.add_argument('-f', '--file', default='ydl.db', help="use sqlite3 FILE (default ydl.db)")
	p.add_argument('--year', help="Year of video")
	p.add_argument('--artist', help="Artist of the video")
	p.add_argument('--title', help="Title of the video")
	p.add_argument('--add', nargs='*', help="Add URL(s) to download")
	p.add_argument('--list', nargs='?', const=True, help="List of lists")
	p.add_argument('--listall', action='store_true', default=False, help="When calling --list, this will list all the videos too")
	# TODO:
	#p.add_argument('--json', action='store_true', default=False, help="Dump output as JSON")
	#p.add_argument('--xml', action='store_true', default=False, help="Dump output as XML")
	
	p.add_argument('--sync', action='store_true', default=False, help="Sync all metadata and playlists (does not download video data)")
	p.add_argument('--ignore-old', action='store_true', default=False, help="Ignore old list items and old videos")
	p.add_argument('--download', action='store_true', default=False, help="Download video")
	args = p.parse_args()

	d = db(args.file)
	d.open()

	# Processing list of URLs
	urls = []

	# List the lists that are known
	if args.list:
		if args.list is True:
			res = d.u.select("*", "")
			rows = [dict(_) for _ in res]
			rows = sorted(rows, key=lambda _: _['name'])

			print("Users (%d):" % len(rows))
			for row in rows:
				sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
				sub_rows = [dict(_) for _ in sub_res]
				sub_cnt = len(sub_rows)

				print("\t%s (%d)" % (row['name'], sub_cnt))

				if args.listall:
					for sub_row in sub_rows:
						subsub_row = d.v.select_one(["title","duration"], "`ytid`=?", [sub_row['ytid']])
						print("\t\t%s: %s (%s)" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration'])))





			res = d.c.select("*", "")
			rows = [dict(_) for _ in res]
			rows = sorted(rows, key=lambda _: _['name'])

			print("Named channels (%d):" % len(rows))
			for row in rows:
				sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
				sub_rows = [dict(_) for _ in sub_res]
				sub_cnt = len(sub_rows)

				print("\t%s (%d)" % (row['name'], sub_cnt))

				if args.listall:
					for sub_row in sub_rows:
						subsub_row = d.v.select_one(["title","duration"], "`ytid`=?", [sub_row['ytid']])
						print("\t\t%s: %s (%s)" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration'])))





			res = d.ch.select("*", "")
			rows = [dict(_) for _ in res]
			rows = sorted(rows, key=lambda _: _['name'])

			print("Unnamed channels (%d):" % len(rows))
			for row in rows:
				sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
				sub_rows = [dict(_) for _ in sub_res]
				sub_cnt = len(sub_rows)

				print("\t%s (%d)" % (row['name'], sub_cnt))

				if args.listall:
					for sub_row in sub_rows:
						subsub_row = d.v.select_one(["title","duration"], "`ytid`=?", [sub_row['ytid']])
						if subsub_row['title'] is None:
							print("\t\t%s: ? (?)" % (sub_row['ytid'],))
						else:
							print("\t\t%s: %s (%s)" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration'])))





			res = d.pl.select("*", "")
			rows = [dict(_) for _ in res]
			rows = sorted(rows, key=lambda _: _['ytid'])

			print("Playlists (%d):" % len(rows))
			for row in rows:
				sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['ytid']], "`idx` asc")
				sub_rows = [dict(_) for _ in sub_res]
				sub_cnt = len(sub_rows)

				print("\t%s (%d)" % (row['ytid'], sub_cnt))

				if args.listall:
					for sub_row in sub_rows:
						subsub_row = d.v.select_one(["title","duration"], "`ytid`=?", [sub_row['ytid']])
						print("\t\t%s: %s (%s)" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration'])))

		else:
			print(args.list)
		sys.exit(0)


	# Check all URLs
	if args.add:
		for url in args.add:
			u = urllib.parse.urlparse(url)
			print(u)

			if u.scheme != 'https':
				print(url)
				print("\t" + "URL only recognized if https")
				sys.exit(-1)

			if u.netloc not in ('www.youtube.com', 'youtube.com', 'youtu.be'):
				print(url)
				print("\t" + "URL not at a recognized host")
				sys.exit(-1)

			if u.path == '/watch':
				# Expect u.query to be 'v=XXXXXXXXXXX'
				q = urllib.parse.parse_qs(u.query)
				if 'v' not in q:
					print(url)
					print("\t" + "Watch URL expected to have a v=XXXXXX query string")
					sys.exit(-1)
				urls.append( ('v', q['v'][0]) )

			if u.path == '/playlist':
				# Expect u.query to be 'list=XXXXXXXXXXXXXXXXXXX'
				q = urllib.parse.parse_qs(u.query)
				if 'list' not in q:
					print(url)
					print("\t" + "Playlist URL expected to have a list=XXXXXX query string")
					sys.exit(-1)
				urls.append( ('p', q['list'][0]) )

			if u.path.startswith('/user/'):
				q = u.path.split('/')
				if len(q) != 3:
					print(url)
					print("\t" + "User URL expected to have a name after /user/")
					sys.exit(-1)
				urls.append( ('u', q[2]) )

			if u.path.startswith('/c/'):
				q = u.path.split('/')
				if len(q) != 3:
					print(url)
					print("\t" + "Channel URL expected to have a channel name after /c/")
					sys.exit(-1)
				urls.append( ('c', q[2]) )

			if u.path.startswith('/channel/'):
				q = u.path.split('/')
				if len(q) != 3:
					print(url)
					print("\t" + "Channel URL expected to have a channel name after /channel/")
					sys.exit(-1)
				urls.append( ('ch', q[2]) )


	d.begin()

	for i,u in enumerate(urls):
		print("%d of %d: %s" % (i+1, len(urls), u[1]))

		if u[0] == 'v':
			o = d.get_video(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")

		elif u[0] == 'u':
			o = d.get_user(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_user(u[1])
				print("\tAdded")

		elif u[0] == 'p':
			o = d.get_playlist(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_playlist(u[1])
				print("\tAdded")

		elif u[0] == 'c':
			o = d.get_channel_named(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_channel_named(u[1])
				print("\tAdded")

		elif u[0] == 'ch':
			o = d.get_channel_unnamed(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_channel_unnamed(u[1])
				print("\tAdded")



		else:
			raise ValueError("Unrecognize URL type %s" % (u,))

	d.commit()

	if args.sync:
		print("Update users")
		sync_users(d, ignore_old=args.ignore_old)

		print("Update unnamed channels")
		sync_channels_unnamed(d, ignore_old=args.ignore_old)

		print("Update named channels")
		sync_channels_named(d, ignore_old=args.ignore_old)

		print("Update playlists")
		sync_playlists(d, ignore_old=args.ignore_old)

		print("Sync all videos")
		sync_videos(d, ignore_old=args.ignore_old)

	if args.download:
		print("Download vides")
		download_videos(d, ignore_old=args.ignore_old)


if __name__ == '__main__':
	_main()

