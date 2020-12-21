
# System
import argparse
import datetime
import glob
import json
import logging
import os
import sys
import traceback
import urllib

# Installed
import sqlite3
import ydl

from sqlitehelper import SH, DBTable, DBCol, DBColROWID

from .util import RSSHelper
from .util import sec_str
from .util import list_to_quoted_csv

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
		# Manually set file names on some as standard ascii translation
		# may make completely gibberish names
		DBTable('vnames',
			DBCol('ytid', 'text'),
			DBCol('name', 'text') # Preferred file name for the given YTID
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
			DBCol('alias', 'text'),
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
		where = ""

		if type(filt) is list and len(filt):
			# Can provide both YTID's and channel/user names to filter by in the same list
			# So search both ytid colum and dname (same as user name, channel name, etc)
			where = "`ytid` in ({0}) or `dname` in ({0})".format(list_to_quoted_csv(filt))

		# If ignore old is desired, then add it to the where clause
		if ignore_old:
			if where: where += " AND "
			where += "`utime` is null"

		res = self.v.select(['rowid','ytid','name','dname','duration','title','skip','ctime','atime','utime'], where)
		return res

	def get_v_dname(self, ytid, absolute=True):
		row = self.v.select_one(['dname','name'], '`ytid`=?', [ytid])
		if row is None:
			raise ValueError("Video with YTID '%s' not found" % ytid)

		if absolute:
			return "%s/%s" % (os.getcwd(), row['dname'])
		else:
			return row['dname']

	def get_v_fname(self, ytid, suffix='mkv'):
		# Get preferred name, if one is set
		row = self.vnames.select_one('name', '`ytid`=?', [ytid])
		alias = None
		if row:
			alias = row['name']

		row = self.v.select_one(['dname','name'], '`ytid`=?', [ytid])
		if row is None:
			raise ValueError("Video with YTID '%s' not found" % ytid)

		return db.format_v_fname(row['dname'], row['name'], alias, ytid, suffix)

	@staticmethod
	def format_v_fname(dname, name, alias, ytid, suffix=None):
		if alias is None:
			fname = name
		else:
			fname = alias

		if suffix is None:
			return "%s/%s/%s-%s" % (os.getcwd(), dname, fname, ytid)
		else:
			return "%s/%s/%s-%s.%s" % (os.getcwd(), dname, fname, ytid, suffix)

	@staticmethod
	def title_to_name(t):
		"""
		Translates the title to a file name.
		There are several banned characters and will collapse whitespace, etc
		"""

		t = t.encode('ascii', errors='ignore').decode('ascii')

		# Preserve these with a hyphen
		t = t.replace(':', '-')
		t = t.replace('/', '-')
		t = t.replace('\\', '-')

		# Just nuke these
		t = t.replace('!', '')
		t = t.replace('?', '')
		t = t.replace('|', '')

		# Collapse all multiple spaces into a single space (each replace will cut # of spaces
		# by half, so assuming no more than 16 spaces
		t = t.replace('  ', ' ')
		t = t.replace('  ', ' ')
		t = t.replace('  ', ' ')
		t = t.replace('  ', ' ')
		t = t.replace('  ', ' ')

		# Get rid of whitespace on the ends
		t = t.strip()

		return t

	@staticmethod
	def alias_coerce(a):
		# Coerce to ascii
		a = a.encode('ascii', errors='ignore').decode('ascii')

		if not a.isalnum():
			raise ValueError("Alias must be alphanumeric")

		return a

def sync_channels_named(args, d, filt, ignore_old, rss_ok):
	"""
	Sync "named" channels (I don't know how else to call them) that are /c/NAME
	as opposed to "unnamed" channels that are at /channel/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(args, d, d.c, filt, 'name', ignore_old, rss_ok, ydl.get_list_c)

def sync_users(args, d, filt, ignore_old, rss_ok):
	"""
	Sync user videos

	Use the database object @d to sync users.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(args, d, d.u, filt, 'name', ignore_old, rss_ok, ydl.get_list_user)

def sync_channels_unnamed(args, d, filt, ignore_old, rss_ok):
	"""
	Sync "unnamed" channels (I don't know how else to call them) that are /channel/NAME
	as opposed to "named" channels that are at /c/NAME
	I don't know the difference but they are not interchangeable.

	Use the database object @d to sync all named channels.
	If @ignore_old is True then skip those that have been sync'ed before.

	If @rss_ok is True then RSS is attempted, otherwise the list is pulled down
	As RSS feeds don't contain the entire history of a list, it is only good for incremental changes.
	"""

	_sync_list(args, d, d.ch, filt, 'name', ignore_old, rss_ok, ydl.get_list_channel)

def sync_playlists(args, d, filt, ignore_old, rss_ok):
	"""
	Sync all playlists.

	Use the database object @d to sync all playlists.
	If @ignore_old is True then skip those that have been sync'ed before.

	@rss_ok is disregarded as playlists don't have RSS feeds; listed to provide consistency (maybe they will change in the future?)
	"""

	# Not applicable to playlists (no RSS)
	rss_ok = False

	_sync_list(args, d, d.pl, filt, 'ytid', ignore_old, rss_ok, ydl.get_list_playlist)

def _sync_list(args, d, d_sub, filt, col_name, ignore_old, rss_ok, ydl_func):
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
		if d_sub.Name == 'ch':
			where = "`{0}` in ({1}) OR `alias` in ({1})".format(col_name, list_to_quoted_csv(filt))
		else:
			where = "`%s` in (%s)" % (col_name, list_to_quoted_csv(filt))

	if ignore_old:
		if len(where): where += " AND "
		where += "`atime` is null"

	# Get list entries
	res = d_sub.select(['rowid',col_name,'atime'], where)

	# Convert to list of dict
	rows = [dict(_) for _ in res.fetchall()]

	# Map ytid/name to row
	mp = {_[col_name]:_ for _ in rows}

	# Supply list name and whether or not to use RSS
	# - If new and rss_ok is False -> rss_ok False
	# - If new and rss_ok is True -> rss_ok False
	# - If old and rss_ok is False-> rss_ok False
	# - If old and rss_ok is True -> rss_ok True
	#
	# if atime is None then it's new, if atim is not None then it's old
	rows = [(v[col_name], v['atime'] is not None and rss_ok,v['rowid']) for k,v in mp.items()]
	rows = sorted(rows, key=lambda _: _[0])

	summary = {
		'done': [],
		'error': [],
		'info': {},
	}

	# Sync the lists
	__sync_list(args, d, d_sub, rows, ydl_func, summary)

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

def __sync_list(args, d, d_sub, rows, f_get_list, summary):
	"""
	Base function that does all the list syncing.

	@d is the database object
	@d_sub is table object in @d
	@rows is a simple array of names & RSS ok flags to print out and reference `vids` entries to
	@f_get_list is a function in ydl library that gets videos for the given list (as this is unique for each list type, it must be supplied
	@rss_ok -- can check RSS first
	@summary -- dictionary to store results of syncing each list
	"""

	for c_name, rss_ok, rowid in rows:
		# Alternate column name (specifically for unnamed channels)
		c_name_alt = None

		# Print the name out to show progress
		if d_sub.Name == 'ch':
			row = d_sub.select_one('alias', "`rowid`=?", [rowid])
			c_name_alt = row[0]

		if c_name_alt:
			print("\t%s -> %s" % (c_name, c_name_alt))
		else:
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
				_c = c_name_alt or c_name

				# Find RSS URL from the list page
				if d_sub.Name == 'c':
					url = RSSHelper.GetByPage('http://www.youtube.com/c/%s' % _c)
				elif d_sub.Name == 'ch':
					url = RSSHelper.GetByPage('http://www.youtube.com/channel/%s' % _c)
				elif d_sub.Name == 'u':
					url = RSSHelper.GetByPage('http://www.youtube.com/user/%s' % _c)
				elif d_sub.Name == 'pl':
					# Playlists don't have RSS feeds
					url = False
				else:
					raise Exception("Unrecognized list type")

				print("\t\tFound RSS from list page, saving to DB (%s)" % url)
				d.begin()
				d.RSS.insert(typ=d_sub.Name, name=_c, url=url, atime=_now())
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
						row = d.vids.select_one('rowid', '`name`=? and `ytid`=?', [c_name_alt or c_name, ytid])
						if not row:
							print("\t\tRSS shows new videos, obtain full list")
							rss_ok = False
							break

		# If rss_ok is still True at this point then no need to check pull list
		# If rss_ok is False, then it was False before checking RSS or was set False for error reasons
		#  or (in particular) there are new videos to check
		if rss_ok and not args.force:
			continue
		else:
			# Fetch full list
			__sync_list_full(args, d, d_sub, rows, f_get_list, summary,   c_name, c_name_alt, new)


def __sync_list_full(args, d, d_sub, rows, f_get_list, summary, c_name, c_name_alt, new):
	"""
	Fetch the full list
	"""

	print("\t\tChecking full list")

	d.begin()
	try:
		cur = f_get_list(c_name, getVideoInfo=False)
		cur = cur[0]

		# Index old values by ytid to the rowid for updating
		res = d.vids.select(["rowid","ytid"], "name=?", [c_name_alt or c_name])
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
		if all_old and not args.force:
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
					d.vids.insert(name=(c_name_alt or c_name), ytid=v['ytid'], idx=v['idx'], atime=_now())

			# Remove all old entries that are no longer on the list by setting index to -1
			# Don't delete so that there retains a mapping of video to original owning list
			for ytid,rowid in old.items():
				d.vids.update({'rowid': '?'}, {'idx': -1})

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
					d.v.insert(ytid=v['ytid'], ctime=n, atime=None, dname=(c_name_alt or c_name), skip=False)

		# upload playlist info
		summary['info'][c_name] = {
			'title': cur['title'],
			'uploader': cur['uploader'],
		}

		# Done with this list
		if c_name not in summary['error']:
			summary['done'].append(c_name)

		d.commit()

	except Exception:
		traceback.print_exc()
		summary['error'].append(c_name)
		# Continue onward, ignore errors
		d.rollback()




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
			d.v.update({"rowid": rowid}, {"atime": _now()})
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
		name = db.title_to_name(ret['title'])

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
		}

		# Do actual update
		d.begin()
		d.v.update({'rowid': rowid}, dat)
		d.commit()

		# Got it
		summary['done'].append(ytid)

def download_videos(d, filt, ignore_old):
	# Get total number of videos in the database
	total = d.v.num_rows()

	print("%d videos in database" % total)

	# See how many are skipped
	total = d.v.num_rows("`skip`=1")

	if type(filt) is list and len(filt):
		print("\tSkipped: %d (total skipped in DB, this flag is ignored if YTID's when channels are specified)" % total)
	else:
		print("\tSkipped: %d" % total)


	# Filter
	where = ""
	if type(filt) is list and len(filt):
		# Can provide both YTID's and channel/user names to filter by in the same list
		# So search both ytid colum and dname (same as user name, channel name, etc)
		where = "(`ytid` in ({0}) or `dname` in ({0})) and `skip`!=1".format(list_to_quoted_csv(filt))
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
		fname = fname.replace('%', '%%')

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
	p.add_argument('--debug', choices=('debug','info','warning','error','critical'), default='error', help="Set logging level")

	p.add_argument('--year', help="Year of video")
	p.add_argument('--artist', help="Artist of the video")
	p.add_argument('--title', help="Title of the video")

	p.add_argument('--add', nargs='*', default=False, help="Add URL(s) to download")
	p.add_argument('--name', nargs='*', default=False, help="Supply a YTID and file name to manually specify it")
	p.add_argument('--alias', nargs='*', default=False, help="Add an alias for unnamed channels")
	p.add_argument('--list', nargs='*', default=False, help="List of lists")
	p.add_argument('--listall', nargs='*', default=False, help="Same as --list but will list all the videos too")
	p.add_argument('--showpath', nargs='*', default=False, help="Show file paths for the given channels or YTID's")
	p.add_argument('--json', action='store_true', default=False, help="Dump output as JSON")
	p.add_argument('--xml', action='store_true', default=False, help="Dump output as XML")

	p.add_argument('--force', action='store_true', default=False, help="Force the action, whatever it may pertain to")
	p.add_argument('--no-rss', action='store_true', default=False, help="Don't use RSS to check status of lists")
	p.add_argument('--skip', nargs='*', help="Skip the specified videos (supply no ids to get a list of skipped)")
	p.add_argument('--unskip', nargs='*', help="Un-skip the specified videos (supply no ids to get a list of not skipped)")
	p.add_argument('--sync', nargs='*', default=False, help="Sync all metadata and playlists (does not download video data)")
	p.add_argument('--sync-list', nargs='*', default=False, help="Sync just the lists (not videos)")
	p.add_argument('--sync-videos', nargs='*', default=False, help="Sync just the videos (not lists)")
	p.add_argument('--ignore-old', action='store_true', default=False, help="Ignore old list items and old videos")
	p.add_argument('--download', nargs='*', default=False, help="Download video")
	args = p.parse_args()

	if args.debug == 'debug':		logging.basicConfig(level=logging.DEBUG)
	elif args.debug == 'info':		logging.basicConfig(level=logging.INFO)
	elif args.debug == 'warning':	logging.basicConfig(level=logging.WARNING)
	elif args.debug == 'error':		logging.basicConfig(level=logging.ERROR)
	elif args.debug == 'critical':	logging.basicConfig(level=logging.CRITICAL)
	else:
		raise ValueError("Unrecognized logging level '%s'" % args.debug)

	d = db(args.file)
	d.open()

	_main_manual(args, d)

	if type(args.showpath) is list:
		_main_showpath(args, d)

	if type(args.list) is list or type(args.listall) is list:
		_main_list(args, d)

	if type(args.add) is list:
		_main_add(args, d)

	if args.skip is not None:
		_main_skip(args, d)

	if args.unskip is not None:
		_main_unskip(args, d)

	if type(args.name) is list:
		_main_name(args, d)

	if type(args.alias) is list:
		_main_alias(args, d)

	if args.sync is not False or args.sync_list is not False:
		_main_sync_list(args, d)

	if args.sync is not False or args.sync_videos is not False:
		_main_sync_videos(args, d)

	if args.download is not False:
		_main_download(args, d)

def _main_manual(args, d):
	"""
	Manually do stuff
	"""

	# Manually coerce the v.name from v.title
	if False:
		res = d.v.select(['rowid','title'], '')
		rows = [dict(_) for _ in res]
		d.begin()
		for row in rows:
			if row['title'] is None: continue

			d.v.update({'rowid': row['rowid']}, {'name': db.title_to_name(row['title'])})
		d.commit()

		sys.exit()

	# Manually coerce file names to v.name, or vnames.name if preent
	if False:
		res = d.v.select(['rowid','ytid'], "`dname`='AdventureswJakeNicole'")
		rows = [dict(_) for _ in res]
		for row in rows:
			ytid = row['ytid']

			# Get directory and preferred name
			dname = d.get_v_dname(ytid)
			name = d.get_v_fname(ytid, suffix=None)

			# Find anything with the matching YTID and rename it
			fs = glob.glob("%s/*%s*" % (dname, ytid))
			for f in fs:
				# Split up by the YTID: everything before is trashed, and file suffix is preserved
				parts = f.rsplit(ytid, 1)

				# Rebuild file name with preferred name, YTID, and the original suffix
				dest = "%s%s" % (name, parts[1])

				if f != dest:
					os.rename(f, dest)

		sys.exit()

	# Fix utime's based on the existence of each completed video (utime=null if absent)
	if False:
		d.begin()
		res = d.v.select(['rowid','ytid','atime','utime'])
		rows = [dict(_) for _ in res]
		for i,row in enumerate(rows):
			print("%d of %d: %s" % (i, len(rows), row['ytid']))

			fname = d.get_v_fname(row['ytid'])
			if os.path.exists(fname):
				# No utime, so needs to be set
				if row['utime'] is None:
					if row['atime'] is None:
						# Not ideal, but needs to be something
						d.v.update({'rowid': row['rowid']}, {'utime': _now()})
					else:
						# No utime so assume atime
						d.v.update({'rowid': row['rowid']}, {'utime': row['atime']})
				else:
					# utime set and that's fine
					pass
			else:
				# utime should be null
				d.v.update({'rowid': row['rowid']}, {'utime': None})

		d.commit()
		sys.exit()



def _main_showpath(args, d):
	"""
	Show paths of all the videos
	"""

	if not len(args.showpath):
		raise KeyError("Must provide a channel to list, use --list to get a list of them")

	where = "(`ytid` in ({0}) or `dname` in ({0}))".format(list_to_quoted_csv(args.showpath))

	res = d.v.select(['rowid','ytid','dname','name','title','duration'], where)
	rows = [dict(_) for _ in res]
	rows = sorted(rows, key=lambda _: _['ytid'])

	for row in rows:
		path = d.get_v_fname(row['ytid'])

		exists = os.path.exists(path)
		if exists:
			print("%s: E %s (%s)" % (row['ytid'],row['title'],sec_str(row['duration'])))
		else:
			print("%s:   %s (%s)" % (row['ytid'],row['title'],sec_str(row['duration'])))

		print("\t%s" % path)
		print()


def _main_list(args, d):
	"""
	List all the user, unnamed channels, named channels, and playlists.
	If --listall supplied then list all of that and the videos for each list.
	"""

	_main_list_user(args, d)
	_main_list_c(args, d)
	_main_list_ch(args, d)
	_main_list_pl(args, d)

def _main_listall(args, d, ytids):
	"""
	List the videos for the YTID's provided in @ytids.
	"""

	# Count number of videos that exist
	counts = 0

	ytids_str = list_to_quoted_csv(ytids)

	# Get video data for all the videos supplied
	# I don't know if there's a query length limit...
	res = d.v.select(["ytid","dname","name","title","duration"], "`ytid` in (%s)" % ytids_str)
	rows = {_['ytid']:_ for _ in res}

	# Map ytids to alias
	res = d.vnames.select(["ytid","name"], "`ytid` in (%s)" % ytids_str)
	aliases = {_['ytid']:_['name'] for _ in res}

	# Iterate over ytids in order provided
	for ytid in ytids:
		# In vids but not v (yet)
		if ytid not in rows:
			print("\t\t%s: ?" % ytid)
			continue

		row = rows[ytid]

		# Check if there's an alias, otherwise format_v_fname takes None for the value
		alias = None
		if ytid in aliases:
			alias = aliases[ytid]

		# All DB querying is done above, so just format it
		path = db.format_v_fname(row['dname'], row['name'], alias, ytid, "mkv")

		# Check if it exists
		exists = os.path.exists(path)
		if exists:
			counts += 1

		if row['title'] is None:
			print("\t\t%s: ?" % ytid)
		else:
			if exists:
				print("\t\t%s: E %s (%s)" % (ytid, row['title'], sec_str(row['duration'])))
			else:
				print("\t\t%s:   %s (%s)" % (ytid, row['title'], sec_str(row['duration'])))

	print("\tExists: %d of %d" % (counts, len(ytids)))


def _main_list_user(args, d):
	"""
	List the users.
	"""

	where = ""
	if type(args.list) is list and len(args.list):
		where = "`name` in (%s)" % list_to_quoted_csv(args.list)
	if type(args.listall) is list and len(args.listall):
		where = "`name` in (%s)" % list_to_quoted_csv(args.listall)

	res = d.u.select("*", where)
	rows = [dict(_) for _ in res]
	rows = sorted(rows, key=lambda _: _['name'])


	print("Users (%d):" % len(rows))
	for row in rows:
		sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
		sub_rows = [dict(_) for _ in sub_res]
		sub_cnt = len(sub_rows)

		print("\t%s (%d)" % (row['name'], sub_cnt))

		# Do only if --listall
		if type(args.listall) is list:
			ytids = [_['ytid'] for _ in sub_rows]
			_main_listall(args, d, ytids)

def _main_list_c(args, d):
	"""
	List the named channels.
	"""

	where = ""
	if type(args.list) is list and len(args.list):
		where = "`name` in (%s)" % list_to_quoted_csv(args.list)
	if type(args.listall) is list and len(args.listall):
		where = "`name` in (%s)" % list_to_quoted_csv(args.listall)

	res = d.c.select("*", where)
	rows = [dict(_) for _ in res]
	rows = sorted(rows, key=lambda _: _['name'])

	print("Named channels (%d):" % len(rows))
	for row in rows:
		sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['name']], "`idx` asc")
		sub_rows = [dict(_) for _ in sub_res]
		sub_cnt = len(sub_rows)

		print("\t%s (%d)" % (row['name'], sub_cnt))

		# Do only if --listall
		if type(args.listall) is list:
			ytids = [_['ytid'] for _ in sub_rows]
			_main_listall(args, d, ytids)

def _main_list_ch(args, d):
	"""
	List the unnamed channels.
	"""

	where = ""
	if type(args.list) is list and len(args.list):
		where = "`name` in ({0}) or `alias` in ({0})".format(list_to_quoted_csv(args.list))
	if type(args.listall) is list and len(args.listall):
		where = "`name` in ({0}) or `alias` in ({0})".format(list_to_quoted_csv(args.listall))

	res = d.ch.select(['rowid','name','alias'], where)
	rows = [dict(_) for _ in res]
	rows = sorted(rows, key=lambda _: _['alias'] or _['name'])

	print("Unnamed channels (%d):" % len(rows))
	for row in rows:
		name = row['alias'] or row['name']

		sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [name], "`idx` asc")
		sub_rows = [dict(_) for _ in sub_res]
		sub_cnt = len(sub_rows)

		if row['alias']:
			print("\t%s -> %s (%d)" % (row['name'], row['alias'], sub_cnt))
		else:
			print("\t%s (%d)" % (row['name'], sub_cnt))

		# Do only if --listall
		if type(args.listall) is list:
			ytids = [_['ytid'] for _ in sub_rows]
			_main_listall(args, d, ytids)

def _main_list_pl(args, d):
	"""
	List the playlists.
	"""

	where = ""
	if type(args.list) is list and len(args.list):
		where = "`ytid` in (%s)" % list_to_quoted_csv(args.list)
	if type(args.listall) is list and len(args.listall):
		where = "`ytid` in (%s)" % list_to_quoted_csv(args.listall)

	res = d.pl.select("*", where)
	rows = [dict(_) for _ in res]
	rows = sorted(rows, key=lambda _: _['ytid'])

	print("Playlists (%d):" % len(rows))
	for row in rows:
		sub_res = d.vids.select(["rowid","ytid"], "`name`=?", [row['ytid']], "`idx` asc")
		sub_rows = [dict(_) for _ in sub_res]
		sub_cnt = len(sub_rows)

		print("\t%s (%d)" % (row['ytid'], sub_cnt))

		# Do only if --listall
		if type(args.listall) is list:
			ytids = [_['ytid'] for _ in sub_rows]
			_main_listall(args, d, ytids)

def _main_add(args, d):
	# Processing list of URLs
	urls = []

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


def _main_skip(args, d):
	"""
	List or add videos to the skip list.
	"""

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

def _main_unskip(args, d):
	"""
	Remove videos from the skip list
	"""

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

def _main_name(args, d):
	"""
	List all the preferred names if --name.
	List information about a single video if --name YTID is provided.
	Set preferred name if --name YTID NAME is provided
	"""

	if len(args.name) == 0:
		res = d.vnames.select(['ytid','name'])
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda x: x['ytid'])

		print("Preferred names (%d):" % len(rows))
		for row in rows:
			sub_row = d.v.select_one('dname', '`ytid`=?', [row['ytid']])

			print("\t%s -> %s / %s" % (row['ytid'], sub_row['dname'], row['name']))

	elif len(args.name) == 1:
		ytid = args.name[0]

		row = d.v.select_one(['rowid','dname','name','title'], '`ytid`=?', [ytid])
		if not row:
			print("No video with YTID '%s' found" % ytid)
			sys.exit()

		print("YTID: %s" % ytid)
		print("Title: %s" % row['title'])
		print("Directory: %s" % row['dname'])
		print("Computed name: %s" % row['name'])

		row = d.vnames.select_one('name', '`ytid`=?', [ytid])
		if row:
			print("Preferred name: %s" % row['name'])
		else:
			print("-- NO PREFERRED NAME SET --")

	elif len(args.name) == 2:
		ytid = args.name[0]

		pref_name = db.title_to_name(args.name[1])
		if pref_name != args.name[1]:
			raise KeyError("Name '%s' is not valid" % args.name[1])

		dname = d.get_v_dname(ytid)

		# Get file name without suffix
		fname = d.get_v_fname(ytid, suffix=None)

		# Find anything with the matching YTID and rename it
		fs = glob.glob("%s/*%s*" % (dname, ytid))
		for f in fs:
			# Split up by the YTID: everything before is trashed, and file suffix is preserved
			parts = f.rsplit(ytid, 1)

			# Rebuild file name with preferred name, YTID, and the original suffix
			dest = "%s/%s-%s%s" % (dname,pref_name, ytid, parts[1])

			os.rename(f, dest)

		d.begin()
		row = d.vnames.select_one('rowid', '`ytid`=?', [ytid])
		if row:
			d.vnames.update({'rowid': row['rowid']}, {'name': pref_name})
		else:
			d.vnames.insert(ytid=ytid, name=pref_name)
		d.commit()

	else:
		print("Too many arguments")


def _main_alias(args, d):
	if len(args.alias) == 0:
		res = d.ch.select(['rowid','name','alias'])
		rows = [dict(_) for _ in res]
		print("Existing channels:")
		for row in rows:
			if row['alias'] is None:
				print("\t%s" % row['name'])
			else:
				print("\t%s -> %s" % (row['name'], row['alias']))
	elif len(args.alias) == 1:
		row = d.ch.select_one(['name','alias'], "`name`=? or `alias`=?", [args.alias[0], args.alias[0]])
		print("Channel: %s" % row['name'])
		print("Alias: %s" % row['alias'])

	elif len(args.alias) == 2:
		res = d.ch.select('*', '`name`=?', [args.alias[1]])
		rows = [dict(_) for _ in res]
		if len(rows):
			raise ValueError("Alias name already used for an unnamed channel: %s" % rows[0]['name'])

		res = d.ch.select('*', '`alias`=?', [args.alias[1]])
		rows = [dict(_) for _ in res]
		if len(rows):
			if rows[0]['name'] == args.alias[0]:
				# Renaming to same alias
				sys.exit()
			else:
				raise ValueError("Alias name already used for an unnamed channel: %s" % rows[0]['name'])

		res = d.c.select('*', '`name`=?', [args.alias[1]])
		rows = [dict(_) for _ in res]
		if len(rows):
			raise ValueError("Alias name already used for an named channel: %s" % rows[0]['name'])

		res = d.u.select('*', '`name`=?', [args.alias[1]])
		rows = [dict(_) for _ in res]
		if len(rows):
			raise ValueError("Alias name already used for a user: %s" % rows[0]['name'])



		pref = db.alias_coerce(args.alias[1])
		if pref != args.alias[1]:
			raise KeyError("Alias '%s' is not valid" % args.name[1])

		row = d.ch.select_one(['rowid','alias'], '`name`=?', [args.alias[0]])
		if row is None:
			raise ValueError("No channel by %s" % args.alias[0])

		# Used for updating vids table
		old_name = args.alias[0]


		# Old and new directory names
		old = os.getcwd() + '/' + args.alias[0]
		new = os.getcwd() + '/' + pref

		# If long ch.name exists on the filesystem then move it to the alias
		if os.path.exists(old):
			os.rename(old, new)

		# If prior ch.alias exists then move it to the new alias
		else:
			# Nope, not there either
			if row['alias'] is None:
				print("No channel directory exists at '%s', making new" % old)
				os.mkdir(new)

			# Move from old to new alias
			else:
				old_name = row['alias']

				old = os.getcwd() + '/' + row['alias']
				new = os.getcwd() + '/' + pref

				if os.path.exists(old):
					os.rename(old, new)

		# Add/update alias to channel
		d.begin()
		d.ch.update({'rowid': row['rowid']}, {'alias': pref})
		d.v.update({'dname': args.alias[0]}, {'dname': pref})
		d.vids.update({'name': old_name}, {'name': pref})
		d.commit()

	else:
		print("Too many variables")


def _main_sync_list(args, d):
	filt = None
	if type(args.sync) is list:			filt = args.sync
	if type(args.sync_list) is list:	filt = args.sync_list

	print("Update users")
	sync_users(args, d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

	print("Update unnamed channels")
	sync_channels_unnamed(args, d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

	print("Update named channels")
	sync_channels_named(args, d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

	print("Update playlists")
	sync_playlists(args, d, filt, ignore_old=args.ignore_old, rss_ok=(not args.no_rss))

def _main_sync_videos(args, d):
	filt = None
	if type(args.sync) is list:			filt = args.sync
	if type(args.sync_videos) is list:	filt = args.sync_videos

	print("Sync all videos")
	sync_videos(d, filt, ignore_old=args.ignore_old)

def _main_download(args, d):
	filt = []
	if type(args.download) is list and len(args.download):
		filt = args.download

	print("Download vides")
	download_videos(d, filt, ignore_old=args.ignore_old)


if __name__ == '__main__':
	_main()

