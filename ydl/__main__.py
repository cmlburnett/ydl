"""
Invoke this with `python3 -m ydl ARGS`.
I have an alias set up in bash to simplify calling it:
		alias ydl='python3 -m ydl'
It managees a sqlite database of channels and videos.
The assumed file name is ydl.db in the current working directory.
	Use -f to specify the file path if not ydl.db.

Common actions:
	Add a new channel
		ydl --add https://www.youtube.com/user/MIT

	Update a channel with a complete list of videos (utilizes RSS if available)
		ydl --sync-list MIT

	Sync only the video information for a channel
		ydl --sync-videos MIT

	Download all (new and old) videos for a channel
		ydl --download MIT

	Download only newest videos for a channel
		ydl --ignore-old --download MIT

	Download only newest videos for a channel and notify by Pushover (configure ~/.pushoverrc first) when done
		ydl --notify --ignore-old --download MIT

	Download a specific video
		ydl --download btZ-VFW4wpY

	List all channels
		ydl --list

	List all channels and their videos
		ydl --listall

	List all videos for a channel
		ydl --listall MIT

	Skip a video (don't download every); do this when there's a problem with downloading the video
		ydl --skip btZ-VFW4wpY

	Un-skip a video
		ydl --unskip btZ-VFW4wpY
"""

# System
import argparse
import datetime
import glob
import json
import logging
import os
import stat
import sys
import traceback
import urllib

# Installed
import ydl

from sqlitehelper import SH, DBTable, DBCol, DBColROWID

from .util import RSSHelper
from .util import sec_str
from .util import list_to_quoted_csv, bytes_to_str
from .util import ytid_hash, ytid_hash_remap

from .fuse import ydl_fuse

try:
	import pushover
except:
	pushover = None

# Path of configuration file for Pushover
PUSHOVER_CFG_FILE = "~/.pushoverrc"
PUSHOVER_CFG_FILE = os.path.expanduser(PUSHOVER_CFG_FILE)

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
	def open(self, rowfactory=None):
		ex = os.path.exists(self.Filename)

		super().open()

		if not ex:
			self.MakeDatabaseSchema()

	def reopen(self):
		super().reopen()

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

	@classmethod
	def format_v_names(cls, dname, name, alias, ytid, suffix=None):
		if alias is None:
			fname = name
		else:
			fname = alias

		if suffix is None:
			return ("%s/%s" % (os.getcwd(), dname), "%s-%s" % (fname, ytid))
		else:
			return ("%s/%s" % (os.getcwd(), dname), "%s-%s.%s" % (fname, ytid, suffix))

	@classmethod
	def format_v_fname(cls, dname, name, alias, ytid, suffix=None):
		return "/".join( cls.format_v_names(dname, name, alias, ytid, suffix) )

	@staticmethod
	def title_to_name(t):
		"""
		Translates the title to a file name.
		There are several banned characters and will collapse whitespace, etc
		"""

		t = t.encode('ascii', errors='ignore').decode('ascii')

		# Strip off leading decimals (glob won't find hidden dot files)
		while t[0] == '.':
			t = t[1:]

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

	def is_skipped_video(self, ytid):
		row = self.v.select_one('skip', '`ytid`=?', [ytid])
		if not row:
			raise ValueError("No video found '%s'" % ytid)

		return row['skip']


def _rename_files(dname, ytid, newname):
	"""
	Rename all files in directory @dname that contains the youtube ID @ytid into the form
		NEWNAME-YTID.SUFFIX
	"""

	# Same base directory
	basedir = os.getcwd()

	# True if any files are moved
	renamed = False

	try:
		# Step into sub directory
		os.chdir(basedir + '/' + dname)

		# Get all files with the YTID in it and all the dot files
		fs = glob.glob('*%s*' % ytid)
		fs2 = glob.glob('.*%s*' % ytid)
		fs = fs + fs2

		# Rename all the files
		for f in fs:
			# Can be "FOO-YTID.SFX"
			# or "FOO-YTID_0.JPG"
			# or "FOO - YTID - STUFF.SFX"
			# or "FOO - YTID - STUFF_0.JPG"
			parts = f.split(ytid)

			# Get the dot suffix of the file
			last = parts[-1].rsplit('.', 1)

			# Things that break the mold in terms of renaming
			if parts[-1] == '.json':
				suffix = '.info.json'
			elif parts[-1] == '.info.json':
				suffix = '.info.json'
			elif last[0].endswith('_0'):
				suffix = '_0.' + last[1]
			elif last[0].endswith('_1'):
				suffix = '_1.' + last[1]
			elif last[0].endswith('_2'):
				suffix = '_2.' + last[1]
			elif last[0].endswith('_3'):
				suffix = '_3.' + last[1]
			elif last[0].endswith('_4'):
				suffix = '_4.' + last[1]
			elif last[0].endswith('_5'):
				suffix = '_5.' + last[1]
			else:
				suffix = '.' + last[1]

			# New pattern is "NEWNAME-YTID.SFX" or "NEWNAME-YTID_0.JPG"
			dest = '%s-%s%s' % (newname, ytid, suffix)

			# If different, print out the file names
			if f != dest:
				print("\t\t%s -> %s" % (f, dest))
				renamed = True

				# Rename
				os.rename(f, dest)

	finally:
		# Go back to the base directory
		os.chdir(basedir)

	# True if any files are renamed
	return renamed


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
	@args -- argparse result object
	@d -- database object
	@d_sub -- database table object for this particular list
	@rows -- list of items to sync, tuple of (name, rss_ok)
	@f_get_list -- function in ydl library to call to get list of videos
	@summary -- dictionary to store results of syncing each list
	@c_name -- column name that uniquely identifies the list (eg, c.name, ch.name, u.name, pl.ytid)
	@c_name_alt -- alternate column name (namely for unnamed channels)
	@new -- list of new YTID's from RSS feed, None otherwise
	"""

	print("\t\tChecking full list")

	d.begin()
	try:
		# Get list of videos using a ydl library function
		cur = f_get_list(c_name, getVideoInfo=False)
		# Passing only one, so get the first (and only) list item
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

		# Get videos that are new and not in the full list
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

					# Ensure items are in the database
					if d.vids.select_one('rowid', '`ytid`=?', [_]) is None:
						d.vids.insert(name=(c_name_alt or c_name), ytid=_, idx=-1, atime=_now())
					if d.v.select_one('rowid', '`ytid`=?', [_]) is None:
						d.v.insert(ytid=_, ctime=None, atime=None, dname=(c_name_alt or c_name), skip=False)
			else:
				print("\t\tAll are old, no updates")

		else:
			# Update or add video to list in vids table
			for v in cur['info']:
				# Update old index
				if v['ytid'] in old:
					#print("\t\t%d: %s (OLD)" % (v['idx'], v['ytid']))
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
			ctime = atime

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

	# Get videos based on filter designed above
	res = d.v.select(['rowid','ytid','title','name','dname','ctime','atime'], where)
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

	# Fetch each video
	for i,ytid in enumerate(ytids):
		row = rows[ytid]

		print("\t%d of %d: %s" % (i+1, len(rows), ytid))

		# Get preferred name, if one is set
		alias_row = d.vnames.select_one('name', '`ytid`=?', [ytid])
		alias = None
		if alias_row:
			alias = alias_row['name']

		# Required
		if row['dname'] is None:
			raise ValueError("Expected dname to be set for ytid '%s'" % row['dname'])

		# If hasn't been updated, then can do sync_videos(ytid) or just download it with ydl
		# then use the info.json file to update the database (saves a call to youtube)
		if row['atime'] is None:
			print("\t\tVideo not synced yet, will get data from info.json file afterward")

			# Keep the real directory name
			dname_real = row['dname']

			# Use name if there happens to be one that is present with atime being null
			if row['name']:
				dname,fname = db.format_v_names(row['dname'], row['name'], alias, row['ytid'])
				# Have to escape the percent signs
				fname = fname.replace('%', '%%')
			else:
				# If no name is present, use TEMP
				dname,fname = db.format_v_names(row['dname'], 'TEMP', alias, row['ytid'])

			# Make subdir if it doesn't exist
			if not os.path.exists(dname):
				os.mkdir(dname)

			# Download mkv file, description, info.json, thumbnails, etc
			try:
				# Escape percent signs
				fname = fname.replace('%', '%%')
				ydl.download(row['ytid'], fname, dname)
			except KeyboardInterrupt:
				# Didn't complete download
				return False
			except:
				# Print it out to see it
				traceback.print_exc()
				# Skip errors, and keep downloading
				continue

			# Look for info.json file that contains title, uplaoder, etc
			fs = glob.glob("%s/*-%s.info.json" % (dname,ytid))
			if not len(fs):
				raise Exception("Downloaded %s to %s/%s but unable to find info.json file" % (ytid, dname, fname))

			# Load in the meta data
			ret = json.load( open(fs[0], 'r') )

			# Squash non-ASCII characters (I don't like emoji in file names)
			name = db.title_to_name(ret['title'])

			# Format
			ctime = row['ctime']
			atime = _now()
			# Updated time is same as accessed time
			utime = atime
			if ctime is None:
				ctime = atime

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
				'utime': utime,
			}

			# Rename TEMP files
			if not row['name']:
				_rename_files(dname_real, ytid, name)

		# Name was present so just download
		else:
			if row['name'] is None:
				raise ValueError("Expected name to be set for ytid '%s'" % row['name'])

			# Format name
			dname,fname = db.format_v_names(row['dname'], row['name'], alias, row['ytid'])
			# Have to escape the percent signs
			fname = fname.replace('%', '%%')

			# Make subdir if it doesn't exist
			if not os.path.exists(dname):
				os.mkdir(dname)

			try:
				ydl.download(row['ytid'], fname, dname)
			except KeyboardInterrupt:
				# Didn't complete download
				return False
			except:
				# Print it out to see it
				traceback.print_exc()
				# Skip errors, and keep downloading
				continue

			dat = {
				'utime': _now()
			}

		# Update data
		d.begin()
		d.v.update({"rowid": row['rowid']}, dat)
		d.commit()

	# Completed download
	return True

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
	p.add_argument('--skip', nargs='*', help="Skip the specified videos (supply no ids to get a list of skipped)")
	p.add_argument('--unskip', nargs='*', help="Un-skip the specified videos (supply no ids to get a list of not skipped)")
	p.add_argument('--info', nargs='*', default=False, help="Print out information about the video")

	p.add_argument('--json', action='store_true', default=False, help="Dump output as JSON")
	p.add_argument('--xml', action='store_true', default=False, help="Dump output as XML")
	p.add_argument('--force', action='store_true', default=False, help="Force the action, whatever it may pertain to")
	p.add_argument('--no-rss', action='store_true', default=False, help="Don't use RSS to check status of lists")

	p.add_argument('--sync', nargs='*', default=False, help="Sync all metadata and playlists (does not download video data)")
	p.add_argument('--sync-list', nargs='*', default=False, help="Sync just the lists (not videos)")
	p.add_argument('--sync-videos', nargs='*', default=False, help="Sync just the videos (not lists)")
	p.add_argument('--ignore-old', action='store_true', default=False, help="Ignore old list items and old videos")
	p.add_argument('--download', nargs='*', default=False, help="Download video")
	p.add_argument('--update-names', nargs='*', default=False, help="Check and update file names to match v.name values (needed if title changed on YouTube after download)")

	p.add_argument('--fuse', nargs=1, help="Initiate FUSE file system fronted by the specified database, provide path to mount to")
	p.add_argument('--fuse-absolute', action='store_true', default=False, help="Sym links are relative by default, pass this to make them absolute paths")

	p.add_argument('--notify', default=False, action='store_true', help="Send a Pushover notification when completed; uses ~/.pushoverrc for config")

	args = p.parse_args()

	if args.debug == 'debug':		logging.basicConfig(level=logging.DEBUG)
	elif args.debug == 'info':		logging.basicConfig(level=logging.INFO)
	elif args.debug == 'warning':	logging.basicConfig(level=logging.WARNING)
	elif args.debug == 'error':		logging.basicConfig(level=logging.ERROR)
	elif args.debug == 'critical':	logging.basicConfig(level=logging.CRITICAL)
	else:
		raise ValueError("Unrecognized logging level '%s'" % args.debug)

	if args.notify:
		if not os.path.exists(PUSHOVER_CFG_FILE):
			print("Unable to send notifications because there is no ~/.pushoverrc configuration file")
			print("Aborting.")
			sys.exit(-1)

		if pushover is None:
			print("Unable to send notifications because pushover is not installed: sudo pip3 install pushover")
			print("Aborting.")
			sys.exit(-1)

	d = db(os.getcwd() + '/' + args.file)
	d.open()

	_main_manual(args, d)

	if args.fuse:
		_main_fuse(args, d, args.fuse_absolute)
		sys.exit()
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

	if type(args.info) is list:
		_main_info(args, d)

	if args.sync is not False or args.sync_list is not False:
		_main_sync_list(args, d)

	if args.sync is not False or args.sync_videos is not False:
		_main_sync_videos(args, d)

	if args.update_names is not False:
		_main_updatenames(args, d)

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
		res = d.v.select(['rowid','ytid'], "`dname`=''")
		rows = [dict(_) for _ in res]
		for row in rows:
			ytid = row['ytid']

			# Get directory and preferred name
			dname = d.get_v_dname(ytid)
			name = d.get_v_fname(ytid, suffix=None)

			# Find anything with the matching YTID and rename it
			fs = glob.glob("%s/*%s*" % (dname, ytid))
			fs2 = glob.glob("%s/.*%s*" % (dname, ytid))
			fs = fs + fs2
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
		res = d.v.select(['rowid','ytid','atime','utime'], "`dname`=''")
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

	if False:
		# Test hashing
		hist = {}
		r = 16
		for i in range(r):
			hist[i] = 0

		res = d.v.select('ytid')
		for row in res:
			a = ytid_hash(row['ytid'], r)
			# Just call to make sure it doesn't error
			b = ytid_hash_remap(row['ytid'], r, r+1)
			hist[a] += 1

		avg = sum(hist.values()) // r
		print(hist)
		print([sum(hist.values()), avg])
		print([_ - avg for _ in hist.values()])

		sys.exit()

def _main_fuse(args, d, absolutepath):
	# Get mount point
	mnt = args.fuse[0]

	# Absolute path it
	mnt = os.path.abspath(mnt)


	# Determine what to prepend to the symlink paths
	if absolutepath:
		rootbase = os.path.abspath( os.path.dirname(d.Filename) )
	else:
		# Get absolute path of the YDL database
		fpath = os.path.abspath(args.file)
		# Get the directory that file is in
		fpath = os.path.dirname(fpath)

		# Get the absolute path of the mount point
		root = os.path.abspath(args.fuse[0])

		# Get the relative path from the
		rootbase = os.path.relpath(fpath, root)

	if not os.path.exists(mnt):
		print("Path %s does not exist" % mnt)
		sys.exit(-1)

	s = os.stat(mnt)
	if not stat.S_ISDIR(s.st_mode):
		print("Path %s is not a directory" % mnt)
		sys.exit(-1)

	ydl_fuse(d, mnt, rootbase)

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
	skipped = 0

	ytids_str = list_to_quoted_csv(ytids)

	# Get video data for all the videos supplied
	# I don't know if there's a query length limit...
	res = d.v.select(["ytid","dname","name","title","duration","skip"], "`ytid` in (%s)" % ytids_str)
	rows = {_['ytid']:_ for _ in res}

	# Map ytids to alias
	res = d.vnames.select(["ytid","name"], "`ytid` in (%s)" % ytids_str)
	aliases = {_['ytid']:_['name'] for _ in res}

	# Iterate over ytids in order provided
	for ytid in ytids:
		# In vids but not v (yet)
		if ytid not in rows:
			print("\t\t%s:   ?" % ytid)
			continue

		row = rows[ytid]

		if row['skip']:
			print("\t\t%s: S" % ytid)
			skipped += 1
			continue

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
			t = row['title']
			t = t.replace('\n', '\\n')
			if exists:
				print("\t\t%s: E %s (%s)" % (ytid, t, sec_str(row['duration'])))
			else:
				print("\t\t%s:   %s (%s)" % (ytid, t, sec_str(row['duration'])))

	print()
	print("\t\tSkipped: %d of %d" % (skipped, len(ytids)))
	print("\t\tExists: %d of %d non-skipped" % (counts, len(ytids)-skipped))

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
				if not os.path.exists(u[1]):
					os.mkdir(u[1])
				print("\tAdded")

		elif u[0] == 'p':
			o = d.get_playlist(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_playlist(u[1])
				if not os.path.exists(u[1]):
					os.mkdir(u[1])
				print("\tAdded")

		elif u[0] == 'c':
			o = d.get_channel_named(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_channel_named(u[1])
				os.mkdir(u[1])
				print("\tAdded")

		elif u[0] == 'ch':
			o = d.get_channel_unnamed(u[1])
			if o:
				print("\tFound")
			else:
				print("\tNot found")
				d.add_channel_unnamed(u[1])
				if not os.path.exists(u[1]):
					os.mkdir(u[1])
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

		# Rename old files
		_rename_files(dname, ytid, pref_name)

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

def _main_info(args, d):
	if not len(args.info):
		_main_info_db(args, d)
	else:
		_main_info_videos(args, d)

def _main_info_db(args, d):
	print("Database information")

	print("\tFile: %s" % d.Filename)

	print()

	cs = d.c.num_rows()
	chs = d.ch.num_rows()
	us = d.u.num_rows()
	pls = d.pl.num_rows()

	print("\tNamed channels: %d" % cs)
	print("\tUnnamed channels: %d" % chs)
	print("\tUsers: %d" % us)
	print("\tPlaylists: %d" % pls)

	vs = d.v.num_rows()
	print("\tVideos: %d" % vs)
	vs = d.v.num_rows('`skip`=1')
	print("\t\tSkipped: %d" % vs)
	vs = d.v.num_rows('`utime` is not null')
	print("\t\tDownloaded: %d" % vs)
	vs = d.vnames.num_rows()
	print("\t\tWith preferred names: %d" % vs)

	row = d.execute("select sum(duration) as duration from v").fetchone()
	days = row['duration'] / (60*60*24.0)
	print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))

def _main_info_videos(args, d):
	ytids = args.info
	print("Showing information for videos (%d):" % len(ytids))

	# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
	ytids = ['-' + _[1:] for _ in ytids if _[0] == '='] + [_ for _ in ytids if _[0] != '=']

	for ytid in ytids:
		row = d.v.select_one('*', '`ytid`=?', [ytid])
		if row is not None:
			_main_info_v(args, d, ytid, row)
			continue

		# Check if named channel
		row = d.c.select_one('*', '`name`=?', [ytid])
		if row is not None:
			print("\tNamed channel %s:" % ytid)
			rows = d.v.select('*', '`dname`=?', [ytid])
			rows = [dict(_) for _ in rows]
			rows = sorted(rows, key=lambda x: x['ytid'])

			row = d.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
			days = row['duration'] / (60*60*24.0)
			print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
			print()

			for row in rows:
				_main_info_v(args, d, row['ytid'], row)

			# Don't, next @ytids entry
			continue

		# Check if unnamed channel
		row = d.ch.select_one('*', '`name`=? or `alias`=?', [ytid,ytid])
		if row is not None:
			print("\tUnnamed channel %s:" % ytid)
			rows = d.v.select('*', '`dname`=?', [ytid])
			rows = [dict(_) for _ in rows]
			rows = sorted(rows, key=lambda x: x['ytid'])

			row = d.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
			days = row['duration'] / (60*60*24.0)
			print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
			print()

			for row in rows:
				_main_info_v(args, d, row['ytid'], row)

			# Don't, next @ytids entry
			continue

		# Check if user
		row = d.u.select_one('*', '`name`=?', [ytid])
		if row is not None:
			print("\tUser %s:" % ytid)
			rows = d.v.select('*', '`dname`=?', [ytid])
			rows = [dict(_) for _ in rows]
			rows = sorted(rows, key=lambda x: x['ytid'])

			row = d.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
			days = row['duration'] / (60*60*24.0)
			print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
			print()

			for row in rows:
				_main_info_v(args, d, row['ytid'], row)

			# Don't, next @ytids entry
			continue

		# Check if playlist
		row = d.pl.select_one('*', '`ytid`=?', [ytid])
		if row is not None:
			print("\tPlaylist %s:" % ytid)
			rows = d.v.select('*', '`dname`=?', [ytid])
			rows = [dict(_) for _ in rows]
			rows = sorted(rows, key=lambda x: x['ytid'])

			row = d.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
			if row['duration'] is None:
				duration = 0
				days = 0.0
			else:
				duration = row['duration']
				days = duration / (60*60*24.0)

			print("\t\tTotal duration: %s (%.2f days)" % (sec_str(duration), days))
			print()

			for row in rows:
				_main_info_v(args, d, row['ytid'], row)

			# Don't, next @ytids entry
			continue

		print("\t%s -- NOT FOUND" % ytid)

def _main_info_v(args, d, ytid, row):
	row = d.v.select_one('*', '`ytid`=?', [ytid])
	if row is None:
		print("\t\tNot found")
		return

	path = db.format_v_fname(row['dname'], row['name'], None, ytid, 'mkv')
	exists = os.path.exists(path)
	size = None
	if exists:
		size = os.stat(path).st_size
		size = '%s (%d bytes)' % (bytes_to_str(size), size)
	else:
		size = ''

	dur = None
	if row['duration']:
		dur = sec_str(row['duration'])

	inf = [
		['YTID', row['ytid']],
		['Title', row['title']],
		['Duration (HH:MM:SS)', dur],
		['Name', row['name']],
		['Directory Name', row['dname']],
		['Uploader', row['uploader']],
		['Upload Time', row['ptime']],
		['Creation Time', row['ctime']],
		['Access Time', row['atime']],
		['Update Time', row['utime']],
		['Skip?', row['skip']],
		['Path', path],
		['Exists?', exists],
		['Size', size],
	]

	# Get maximum length of the keys
	maxlen = max( [len(_[0]) for _ in inf] )

	# Print out the information
	for k,v in inf:
		print('\t\t' + ("%0" + str(maxlen) + "s: %s") % (k,v))
	print()


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

	# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
	filt = ['-' + _[1:] for _ in filt if _[0] == '='] + [_ for _ in filt if _[0] != '=']

	print("Sync all videos")
	sync_videos(d, filt, ignore_old=args.ignore_old)

def _main_updatenames(args, d):
	print("Updating file names to v.name or with preferred name")

	where = '`skip`=0'
	if type(args.update_names) is list:
		# Filter
		where += " AND (`ytid` in ({0}) or `dname` in ({0}))".format( list_to_quoted_csv(args.update_names) )

	res = d.v.select(['rowid','ytid','dname','name'], where)

	basedir = os.getcwd()

	summary = {
		'same': [],
		'change': [],
	}

	rows = [dict(_) for _ in res]
	for i,row in enumerate(rows):
		ytid = row['ytid']
		dname = row['dname']
		name = row['name']
		if name is None:
			name = 'TEMP'

		# Get preferred name, if one is set
		sub_row = d.vnames.select_one('name', '`ytid`=?', [ytid])
		if sub_row:
			name = sub_row['name']

		print("\t%d of %d: %s" % (i+1, len(rows), row['ytid']))

		# Find everything with that YTID (glob ignores dot files)
		renamed = _rename_files(dname, ytid, name)
		if renamed:
			summary['change'].append(ytid)
		else:
			summary['same'].append(ytid)

	print("Same: %d" % len(summary['same']))
	print("Changed: %d" % len(summary['change']))

def _main_download(args, d):
	filt = []
	if type(args.download) is list and len(args.download):
		filt = args.download
		# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
		filt = ['-' + _[1:] for _ in filt if _[0] == '='] + [_ for _ in filt if _[0] != '=']

	print("Download videos")
	try:
		ret = download_videos(d, filt, ignore_old=args.ignore_old)
	except Exception as e:
		ret = e

	# Send notificaiton via Pushover
	if args.notify:
		# Send osmething useful but short
		msg = ",".join(filt)
		if len(msg) > 32:
			msg = msg[:32] + '...'

		if ret == True:
			msg = "Download completed: %s" % msg

		elif ret == False:
			msg = "Download aborted: %s" % msg

		elif type(ret) is Exception:
			errmsg = str(ret)
			if len(errmsg) > 32:
				errmsg = errmsg[:32] + '...'

			msg = "Dwonload aborted with exception (%s) for %s" % (errmsg, msg)
		else:
			msg = "Download something: %s" % msg

		pushover.Client().send_message(msg, title="ydl")
		print('notify: %s' % msg)


if __name__ == '__main__':
	_main()

