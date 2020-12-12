
# System
import argparse
import datetime
import json
import logging
import os
import re
import sys
import time
import traceback
import urllib

# Installed
import sqlite3
import ydl

logging.basicConfig(level=logging.ERROR)

from sqlitehelper import SH, DBTable, DBCol, DBColROWID

from .util import RSSHelper
from .util import sec_str

def _now():
	""" Now """
	return datetime.datetime.utcnow()

class db(SH):
	"""
	DB interface that wraps sqlite3 using the sqlitehelper library.
	Schema is declared as below.
	"""

	__schema__ = [
		DBTable('v',
			DBColROWID(),
			DBCol('ytid', 'text'),
			DBCol('name', 'text'),  # File name of the saved video
			DBCol('dname', 'text'), # Directory the file will be saved in (based on which list it is added from first)
			DBCol('duration', 'integer'),
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ptime', 'datetime'), # Upload time to youtube (whatever they say it is)
			DBCol('ctime', 'datetime'), # Creation time (first time this video was put in the list)
			DBCol('atime', 'datetime'), # Access time (last time this video was touched)
			DBCol('utime', 'datetime'), # Update time (last time anything for the video was downloaded)
			DBCol('skip', 'bool'),

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
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Named channels
		DBTable('c',
			DBCol('name', 'text'),
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Unnamed channels
		DBTable('ch',
			DBCol('name', 'text'),
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		# Users
		DBTable('u',
			DBCol('name', 'text'),
			DBCol('title', 'text'),
			DBCol('uploader', 'text'),
			DBCol('ctime', 'datetime'),
			DBCol('atime', 'datetime')
		),
		DBTable('vids',
			DBCol('name', 'text'),
			DBCol('ytid', 'text'),
			DBCol('idx', 'integer'),
			DBCol('atime', 'datetime'),
		),

		# RSS feeds
		DBTable('RSS',
			DBCol('typ', 'text'),
			DBCol('name', 'text'),
			DBCol('url', 'text'),
			DBCol('atime', 'datetime'), # Last time the RSS feed was sync'ed
		),
	]
	def open(self):
		ex = os.path.exists(self.Filename)

		super().open()

		if not ex:
			self.MakeDatabaseSchema()


	def get_user(self, name):
		return self.u.select_one("*", "`name`=?", [name])

	def get_playlist(self, ytid):
		return self.pl.select_one("*", "`ytid`=?", [ytid])

	def get_channel_named(self, name):
		return self.c.select_one("*", "`name`=?", [name])

	def get_channel_unnamed(self, name):
		return self.ch.select_one("*", "`name`=?", [name])


	def add_user(self, name):
		return self.u.insert(name=name, ctime=_now())

	def add_playlist(self, ytid):
		return self.pl.insert(ytid=ytid, ctime=_now())

	def add_channel_named(self, name):
		return self.c.insert(name=name, ctime=_now())

	def add_channel_unnamed(self, name):
		return self.ch.insert(name=name, ctime=_now())


	def get_v(self, filt, ignore_old):
		if type(filt) is list and len(filt):
			# Can provide both YTID's and channel/user names to filter by in the same list
			# So search both ytid colum and dname (same as user name, channel name, etc)
			where = "`ytid` in ({0}) or `dname` in ({0})".format(",".join( ["'%s'" % _ for _ in filt] ))

		# If ignore old is desired, then add it to the where clause
		if ignore_old:
			if where: where += " AND "
			where += "`utime` is null"

		print(['where', where])
		res = self.v.select(['rowid','ytid','name','dname','duration','title','skip','ctime','atime','utime'], where)
		return res

def sync_channels_named(d, filt, ignore_old, rss_ok):
	"""
	Sync "named" channels (I don't know how else to call them) that are /c/NAME
	as opposed to "unnamed" channels that are at /channel/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(d, d.c, filt, 'name', ignore_old, rss_ok, ydl.get_list_c)

def sync_users(d, filt, ignore_old, rss_ok):
	"""
	Sync user videos

	Use the database object @d to sync users.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(d, d.u, filt, 'name', ignore_old, rss_ok, ydl.get_list_user)

def sync_channels_unnamed(d, filt, ignore_old, rss_ok):
	"""
	Sync "unnamed" channels (I don't know how else to call them) that are /channel/NAME
	as opposed to "named" channels that are at /c/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(d, d.ch, filt, 'name', ignore_old, rss_ok, ydl.get_list_channel)

def sync_playlists(d, filt, ignore_old, rss_ok):
	"""
	Sync all playlists.

	Use the database object @d to sync all playlists.
	If @ignore_old is True then skip those that have been sync'ed before.

	@rss_ok is disregarded as playlists don't have RSS feeds; listed to provide consistency (maybe they will change in the future?)
	"""

	# Not applicable to playlists (no RSS)
	rss_ok = False

	_sync_list(d, d.pl, filt, 'ytid', ignore_old, rss_ok, ydl.get_list_playlist)

def _sync_list(d, d_sub, filt, col_name, ignore_old, rss_ok, ydl_func):
	"""
	Sub helper function
	@d -- main database object
	@d_sub -- sub object that is table specific for the list being updated
	@filt -- list of names to filter by
	@col_name -- name of the column in @d_sub that is the name of the list (eg, ytid, name)
	@ignore_old -- only look at new stuff
	@rss_ok -- can check list current-ness by using RSS
	@ydl_func -- function in ydl library to call to sync the list

	This calls __sync_list further.
	"""

	# Filter based on atime being null if @ignore_old is True
	where = ""
	if type(filt) is list and len(filt):
		# FIXME: need to pass by value
		where = "`%s` in (%s)" % (col_name, ",".join( ["'%s'" % _ for _ in filt] ))

	if ignore_old:
		if len(where): where += " AND "
		where += "`atime` is null"

	res = d_sub.select(['rowid',col_name,'atime'], where)

	# Convert to list of dict
	rows = [dict(_) for _ in res]

	# Map ytid/name to row
	mp = {_[col_name]:_ for _ in rows}

	# Supply list name and whether or not to use RSS
	# - If new and rss_ok is False -> rss_ok False
	# - If new and rss_ok is True -> rss_ok False
	# - If old and rss_ok is False-> rss_ok False
	# - If old and rss_ok is True -> rss_ok True
	#
	# if atime is None then it's new, if atim is not None then it's old
	rows = [(v[col_name], v['atime'] is not None and rss_ok) for k,v in mp.items()]
	rows = sorted(rows, key=lambda _: _[0])

	summary = {
		'done': [],
		'error': [],
		'info': {},
	}

	# Sync the lists
	__sync_list(d, d_sub, rows, ydl_func, summary)

	print("\tDone: %d" % len(summary['done']))
	print("\tError: %d" % len(summary['error']))
	for ytid in summary['error']:
		print("\t\t%s" % ytid)

	# Update atimes
	d.begin()
	for ytid in summary['done']:
		rowid = mp[ytid]['rowid']

		d_sub.update({'rowid': rowid}, {'atime': _now(), 'title': summary['info'][ytid]['title'], 'uploader': summary['info'][ytid]['uploader']})
	d.commit()

def __sync_list(d, d_sub, rows, f_get_list, summary):
	"""
	Base function that does all the list syncing.

	@d is the database object
	@d_sub is table object in @d
	@rows is a simple array of names & RSS ok flags to print out and reference `vids` entries to
	@f_get_list is a function in ydl library that gets videos for the given list (as this is unique for each list type, it must be supplied
	@rss_ok -- can check RSS first
	@summary -- dictionary to store results of syncing each list
	"""

	for c_name, rss_ok in rows:
		# Print the name out to show progress
		print("\t%s" % c_name)

		# New list of YTID's from RSS, None if not processed
		new = None

		# If ok to check RSS, start there and if all video sthere are in the database
		# then no need to pull down the full list
		if rss_ok:
			row = d.RSS.select_one("url", "`typ`=? and `name`=?", [d_sub.Name, c_name])
			if row:
				# Found RSS url, just use that
				url = row['url']
			else:
				# Find RSS URL from the list page
				if d_sub.Name == 'c':
					url = RSSHelper.GetByPage('http://www.youtube.com/c/%s' % c_name)
				elif d_sub.Name == 'ch':
					url = RSSHelper.GetByPage('http://www.youtube.com/channel/%s' % c_name)
				elif d_sub.Name == 'u':
					url = RSSHelper.GetByPage('http://www.youtube.com/user/%s' % c_name)
				elif d_sub.Name == 'pl':
					# Playlists don't have RSS feeds
					url = False
				else:
					raise Exception("Unrecognized list type")

				print("\t\tFound RSS from list page, saving to DB (%s)" % url)
				d.begin()
				d.RSS.insert(typ=d_sub.Name, name=c_name, url=url, atime=_now())
				d.commit()

			# Check that url was found
			if url == False:
				print("\t\tCan't get RSS feed")
				# Unable to get rss feed
				rss_ok = False
			else:
				print("\t\tChecking RSS (%s)" % url)
				ret = RSSHelper.ParseRSS_YouTube(url)
				if ret:
					present = []
					logging.basicConfig(level=logging.DEBUG)

					# Save list of new YTID's
					new = ret['ytids']

					for ytid in ret['ytids']:
						row = d.vids.select_one('rowid', '`name`=? and `ytid`=?', [c_name, ytid])
						if not row:
							print("\t\tRSS shows new videos, obtain full list")
							rss_ok = False
							break
		# If rss_ok is still True at this point then no need to check pull list
		# If rss_ok is False, then it was False before checking RSS or was set False for error reasons
		#  or (in particular) there are new videos to check
		if rss_ok:
			continue
		else:
			print("\t\tChecking full list")

		d.begin()
		try:
			cur = f_get_list(c_name, getVideoInfo=False)
			cur = cur[0]

			# Index old values by ytid to the rowid for updating
			res = d.vids.select(["rowid","ytid"], "name=?", [c_name])
			old = {r['ytid']:r['rowid'] for r in res}

			# Check if all are old, then skip updating
			all_old = True
			for v in cur['info']:
				if v['ytid'] not in old:
					all_old = False
					break

			# Get vieos that are new and not in the full list
			if new:
				weird_diff = set(new) - set([_['ytid'] for _ in cur['info']])
			else:
				weird_diff = []

			# At least one is new
			if all_old:
				if weird_diff:
					print("\t\tFound videos in RSS but not in video list, probably upcoming videos (%d)" % len(weird_diff))
					for _ in weird_diff:
						print("\t\t\t%s" % _)
				else:
					print("\t\tAll are old, no updates")
			else:
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
					d.vids.delete({'rowid': '?'}, [rowid])

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
						d.v.insert(ytid=v['ytid'], ctime=n, atime=None, dname=c_name, skip=False)

			# upload playlist info
			summary['info'][c_name] = {
				'title': cur['title'],
				'uploader': cur['uploader'],
			}

		except Exception:
			traceback.print_exc()
			summary['error'].append(c_name)
			# Continue onward, ignore errors

		# Done with this list
		if c_name not in summary['error']:
			summary['done'].append(c_name)

		d.commit()





def sync_videos(d, filt, ignore_old):
	"""
	Sync all videos in the database @d and if @ignore_old is True then don't sync
	those videos that have been sync'ed before.
	"""

	# Get videos
	res = d.get_v(filt, ignore_old)

	# Convert rows to dictionaries
	rows = [dict(_) for _ in res]
	# Sort by YTID to be consistent
	rows = sorted(rows, key=lambda x: x['ytid'])
	print(rows)

	summary = {
		'done': [],
		'error': [],
		'paymentreq': [],
	}

	try:
		_sync_videos(d, ignore_old, summary, rows)
	except KeyboardInterrupt:
		# Don't show exception
		return
	finally:
		print()
		print()
		print()
		print("Total videos: %d" % len(rows))
		print("Completed: %d" % len(summary['done']))
		print("Payment required (%d):" % len(summary['paymentreq']))
		for ytid in summary['paymentreq']:
			print("\t%s" % ytid)
		print("Other errors (%d):" % len(summary['error']))
		for ytid in summary['error']:
			print("\t%s" % ytid)

def _sync_videos(d, ignore_old, summary, rows):
	# Iterate over videos
	for i,row in enumerate(rows):
		ytid = row['ytid']
		rowid = row['rowid']
		ctime = row['ctime']
		skip = row['skip']

		# print to the screen to show progress
		print("\t%d of %d: %s" % (i+1,len(rows), ytid))

		# If instructed to skip, then skip
		# This can be done if the video is on a playlist, etc that is not available to download
		if skip:
			print("\t\tSkipping")
			# This marks it as at least looked at, otherwise repeated --sync --ignore-old will keep checking
			d.v.update({"rowid": rowid}, {"utime": _now()})
			continue

		# Get video information
		try:
			ret = ydl.get_info_video(ytid)
		except KeyboardInterrupt:
			# Pass it down
			raise
		except ydl.PaymentRequiredException:
			summary['paymentreq'].append(ytid)
			continue
		except Exception as e:
			traceback.print_exc()
			summary['error'].append(ytid)
			continue

		# Squash non-ASCII characters (I don't like emoji in file names)
		name = ret['title'].encode('ascii', errors='ignore').decode('ascii')
		# Get rid of characters that are bad for file names
		name = name.replace(':', '-')
		name = name.replace('/', '-')
		name = name.replace('\\', '-')
		name = name.replace('!', '')
		name = name.replace('?', '')
		name = name.replace('|', '')
		# Collapse all multiple spaces into a single space (each replace will cut # of spaces
		# by half, so assuming no more than 16 spaces
		name = name.replace('  ', ' ')
		name = name.replace('  ', ' ')
		name = name.replace('  ', ' ')
		name = name.replace('  ', ' ')
		name = name.replace('  ', ' ')
		# Get rid of trailing whitespace
		name = name.strip()

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

		# Got it
		summary['done'].append(ytid)

def download_videos(d, filt, ignore_old):
	# Get total number of videos in the database
	res = d.v.select(['rowid'], "")
	total = len(res.fetchall())

	print("%d videos in database" % total)

	# See how many are skipped
	res = d.v.select(['rowid'], "`skip`=1")
	total = len(res.fetchall())

	if type(filt) is list and len(filt):
		print("\tSkipped: %d (total skipped in DB, this flag is ignored if YTID's when channels are specified)" % total)
	else:
		print("\tSkipped: %d" % total)


	# Filter
	where = ""
	if type(filt) is list and len(filt):
		# Can provide both YTID's and channel/user names to filter by in the same list
		# So search both ytid colum and dname (same as user name, channel name, etc)
		where = "(`ytid` in ({0}) or `dname` in ({0}))".format(",".join( ["'%s'" % _ for _ in filt] ))
	else:
		# Enable skip if not filtering
		where = "`skip`!=1"

	if ignore_old:
		print("Ignoring old videos")
		if where: where += " AND "
		where += "`utime` is null"

	res = d.v.select(['rowid','ytid','title','name','dname'], where)
	rows = res.fetchall()

	if (type(filt) is list and len(filt)) or ignore_old:
		print("\tFiltered down to %d" % len(rows))
	# Pad out a line
	print()

	# Convert to dictionaries and index by ytid
	rows = [dict(_) for _ in rows]
	rows = {_['ytid']:_ for _ in rows}

	# Sort by ytids to consistently download in same order
	ytids = list(rows.keys())
	ytids = sorted(ytids)

	for i,ytid in enumerate(ytids):
		row = rows[ytid]

		print("\t%d of %d: %s" % (i+1, len(rows), row['ytid']))

		cwd = os.getcwd()
		dname = os.path.join(cwd, row['dname'])

		# Append YTID to the file name
		fname = row['name'] + '-' + row['ytid']

		# Make subdir if it doesn't exist
		if not os.path.exists(dname):
			os.mkdir(dname)

		try:
			ydl.download(row['ytid'], fname, dname)
		except KeyboardInterrupt:
			break
		except:
			# Print it out to see it
			traceback.print_exc()
			# Skip errors, and keep downloading
			continue

		d.begin()
		d.v.update({"rowid": row['rowid']}, {'name': row['name'], 'utime': _now()})
		d.commit()


def _main():
	""" Main function called from invoking the library """

	p = argparse.ArgumentParser()
	p.add_argument('-f', '--file', default='ydl.db', help="use sqlite3 FILE (default ydl.db)")
	p.add_argument('--stdin', action='store_true', default=False, help="Accept input on STDIN for parameters instead of arguments")
	p.add_argument('--year', help="Year of video")
	p.add_argument('--artist', help="Artist of the video")
	p.add_argument('--title', help="Title of the video")

	p.add_argument('--add', nargs='*', default=False, help="Add URL(s) to download")
	p.add_argument('--list', nargs='*', default=False, help="List of lists")
	p.add_argument('--listall', nargs='*', default=False, help="Same as --list but will list all the videos too")
	p.add_argument('--showpath', nargs='*', default=False, help="Show file paths for the given channels or YTID's")
	p.add_argument('--json', action='store_true', default=False, help="Dump output as JSON")
	p.add_argument('--xml', action='store_true', default=False, help="Dump output as XML")

	p.add_argument('--no-rss', action='store_true', default=False, help="Don't use RSS to check status of lists")
	p.add_argument('--skip', nargs='*', help="Skip the specified videos (supply no ids to get a list of skipped)")
	p.add_argument('--unskip', nargs='*', help="Un-skip the specified videos (supply no ids to get a list of not skipped)")
	p.add_argument('--sync', nargs='*', default=False, help="Sync all metadata and playlists (does not download video data)")
	p.add_argument('--sync-list', nargs='*', default=False, help="Sync just the lists (not videos)")
	p.add_argument('--sync-videos', nargs='*', default=False, help="Sync just the videos (not lists)")
	p.add_argument('--ignore-old', action='store_true', default=False, help="Ignore old list items and old videos")
	p.add_argument('--download', nargs='*', default=False, help="Download video")
	args = p.parse_args()

	d = db(args.file)
	d.open()

	# Show paths of videos
	if type(args.showpath) is list:
		where = "(`ytid` in ({0}) or `dname` in ({0}))".format(",".join( ["'%s'" % _ for _ in args.showpath] ))

		res = d.v.select(['rowid','ytid','dname','name','title','duration'], where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['ytid'])

		for row in rows:
			cwd = os.getcwd()
			dname = os.path.join(cwd, row['dname'])

			# Append YTID to the file name
			fname = row['name'] + '-' + row['ytid']

			path = dname + '/' + fname + '.mkv'

			exists = os.path.exists(path)
			if exists:
				print("%s: %s (%s) EXISTS" % (row['ytid'],row['title'],sec_str(row['duration'])))
			else:
				print("%s: %s (%s)" % (row['ytid'],row['title'],sec_str(row['duration'])))

			print("\t%s" % path)
			print()


	# List the lists that are known
	if args.list is not False or args.listall is not False:
		where = ""
		where_pl = ""
		if type(args.list) is list and len(args.list):
			where = "`name` in (%s)" % ",".join( ["'%s'" % _ for _ in args.list] )
			where_pl = "`ytid` in (%s)" % ",".join( ["'%s'" % _ for _ in args.list] )
		if type(args.listall) is list and len(args.listall):
			where = "`name` in (%s)" % ",".join( ["'%s'" % _ for _ in args.listall] )
			where_pl = "`ytid` in (%s)" % ",".join( ["'%s'" % _ for _ in args.listall] )

		res = d.u.select("*", where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['name'])


		print("Users (%d):" % len(rows))
		for row in rows:
			sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
			sub_rows = [dict(_) for _ in sub_res]
			sub_cnt = len(sub_rows)

			print("\t%s (%d)" % (row['name'], sub_cnt))

			if type(args.listall) is list:
				counts = 0

				for sub_row in sub_rows:
					subsub_row = d.v.select_one(["dname","name","title","duration"], "`ytid`=?", [sub_row['ytid']])

					path = "%s/%s/%s-%s.mkv" % (os.getcwd(), subsub_row['dname'], subsub_row['name'], sub_row['ytid'])
					exists = os.path.exists(path)
					if exists:
						counts += 1

					print("\t\t%s: %s (%s)%s" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration']), exists and " EXISTS" or ""))


				print("\tExists: %d of %d" % (counts, len(sub_rows)))




		res = d.c.select("*", where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['name'])

		print("Named channels (%d):" % len(rows))
		for row in rows:
			sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
			sub_rows = [dict(_) for _ in sub_res]
			sub_cnt = len(sub_rows)

			print("\t%s (%d)" % (row['name'], sub_cnt))

			if type(args.listall) is list:
				counts = 0

				for sub_row in sub_rows:
					subsub_row = d.v.select_one(["dname","name","title","duration"], "`ytid`=?", [sub_row['ytid']])

					exists = False
					if subsub_row:
						path = "%s/%s/%s-%s.mkv" % (os.getcwd(), subsub_row['dname'], subsub_row['name'], sub_row['ytid'])
						exists = os.path.exists(path)
						if exists:
							counts += 1

					if not exists:
						if subsub_row is None:
							print("\t\t%s: ?" % (sub_row['ytid'],))
						else:
							print("\t\t%s: %s (%s)%s" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration']), exists and " EXISTS" or ""))

				print("\tExists: %d of %d" % (counts, len(sub_rows)))



		res = d.ch.select("*", where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['name'])

		print("Unnamed channels (%d):" % len(rows))
		for row in rows:
			sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
			sub_rows = [dict(_) for _ in sub_res]
			sub_cnt = len(sub_rows)

			print("\t%s (%d)" % (row['name'], sub_cnt))

			if type(args.listall) is list:
				for sub_row in sub_rows:
					subsub_row = d.v.select_one(["dname","name","title","duration"], "`ytid`=?", [sub_row['ytid']])
					if subsub_row['title'] is None:
						print("\t\t%s: ? (?)" % (sub_row['ytid'],))
					else:
						exists = False
						if subsub_row:
							path = "%s/%s/%s-%s.mkv" % (os.getcwd(), subsub_row['dname'], subsub_row['name'], sub_row['ytid'])
							exists = os.path.exists(path)
							if exists:
								counts += 1

						print("\t\t%s: %s (%s)%s" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration']), exists and " EXISTS" or ""))

				print("\tExists: %d of %d" % (counts, len(sub_rows)))





		res = d.pl.select("*", where_pl)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['ytid'])

		print("Playlists (%d):" % len(rows))
		for row in rows:
			sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['ytid']], "`idx` asc")
			sub_rows = [dict(_) for _ in sub_res]
			sub_cnt = len(sub_rows)

			print("\t%s (%d)" % (row['ytid'], sub_cnt))

			if type(args.listall) is list:
				for sub_row in sub_rows:
					subsub_row = d.v.select_one(["dname","name","title","duration"], "`ytid`=?", [sub_row['ytid']])

					exists = False
					if subsub_row:
						path = "%s/%s/%s-%s.mkv" % (os.getcwd(), subsub_row['dname'], subsub_row['name'], sub_row['ytid'])
						exists = os.path.exists(path)
						if exists:
							counts += 1

					print("\t\t%s: %s (%s)%s" % (sub_row['ytid'], subsub_row['title'], sec_str(subsub_row['duration']), exists and " EXISTS" or ""))

				print("\tExists: %d of %d" % (counts, len(sub_rows)))


	# Processing list of URLs
	urls = []

	# Check all URLs
	if type(args.add) is list:
		if args.stdin:
			vals = [_.strip() for _ in sys.stdin.readlines()]
		else:
			vals = args.add

		for url in vals:
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



	if args.skip is not None:
		if not len(args.skip):
			res = d.v.select("ytid", "`skip`=?", [True])
			ytids = [_['ytid'] for _ in res]
			ytids = sorted(ytids)

			if args.json:
				print(json.dumps(ytids))
			elif args.xml:
				raise NotImplementedError("XML not implemented yet")
			else:
				# FIXME: abide by --json and --xml
				print("Videos marked skip (%d):" % len(ytids))
				for ytid in ytids:
					print("\t%s" % ytid)
		else:
			# This could signify STDIN contains json or xml to intrepret as ytids???
			if args.json:
				raise NotImplementedError("--json not meaningful when adding skipped videos")
			if args.xml:
				raise NotImplementedError("--xml not meaningful when adding skipped videos")

			ytids = list(set(args.skip))
			print("Marking videos to skip (%d):" % len(ytids))


			d.begin()
			for ytid in ytids:
				print("\t%s" % ytid)
				row = d.v.select_one("rowid", "`ytid`=?", [ytid])
				d.v.update({"rowid": row['rowid']}, {"skip": True})
			d.commit()

	if args.unskip is not None:
		if not len(args.unskip):
			res = d.v.select("ytid", "`skip`=?", [False])
			ytids = [_['ytid'] for _ in res]
			ytids = sorted(ytids)

			if args.json:
				print(json.dumps(ytids))
			elif args.xml:
				raise NotImplementedError("XML not implemented yet")
			else:
				print("Videos NOT marked skip (%d):" % len(ytids))
				for ytid in ytids:
					print("\t%s" % ytid)
		else:
			# This could signify STDIN contains json or xml to intrepret as ytids???
			if args.json:
				raise NotImplementedError("--json not meaningful when removing skipped videos")
			if args.xml:
				raise NotImplementedError("--xml not meaningful when removed skipped videos")

			ytids = list(set(args.unskip))
			print("Marking videos to not skip (%d):" % len(ytids))

			d.begin()
			for ytid in ytids:
				print("\t%s" % ytids)
				row = d.v.select_one("rowid", "`ytid`=?", [ytid])
				d.v.update({"rowid": row['rowid']}, {"skip": False})
			d.commit()

	if args.sync is not False or args.sync_list is not False:
		filt = None
		if type(args.sync) is list:			filt = args.sync
		if type(args.sync_list) is list:	filt = args.sync_list

		print("Update users")
		sync_users(d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

		print("Update unnamed channels")
		sync_channels_unnamed(d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

		print("Update named channels")
		sync_channels_named(d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

		print("Update playlists")
		sync_playlists(d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

	if args.sync is not False or args.sync_videos is not False:
		filt = None
		if type(args.sync) is list:			filt = args.sync
		if type(args.sync_videos) is list:	filt = args.sync_videos

		print("Sync all videos")
		sync_videos(d, filt, ignore_old=args.ignore_old)

	if args.download is not False:
		filt = []
		if type(args.download) is list and len(args.download):
			filt = args.download

		print("Download vides")
		download_videos(d, filt, ignore_old=args.ignore_old)


if __name__ == '__main__':
	_main()

