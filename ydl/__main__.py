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

	To sleep a video when it is not yet released (functions like skipping until the time is lapsed)
		ydl --sleep btZ-VFW4wpY TIME

		where TIME can be absolute in YYYY-MM-DD HH:MM:SS format or relative format
		where d+N in days, h+N in hours, m+N in minutes, s+N in seconds relative to curent time
		and all times are in UTC.

	To see all sleeping videos sorted by soonest-to-expire first
		ydl --sleep

	To un-sleep a video
		ydl --unsleep btZ-VFW4wpY

	Mount a FUSE filesystem to list channels and videos
		ydl --fuse /mnt/ydl

	Mount a FUSE filesystem with sym links use absolute paths
		ydl --fuse-absolute /mnt/ydl
"""

# System
import argparse
import datetime
import glob
import json
import logging
import os
import stat
import subprocess
import sys
import tempfile
import traceback
import urllib

# Installed
import requests
import ydl
import youtube_dl

import mkvxmlmaker

from .util import RSSHelper
from .util import sec_str, t_to_sec
from .util import list_to_quoted_csv, bytes_to_str
from .util import ytid_hash, ytid_hash_remap
from .util import inputopts
from .util import print_2col
from .util import title_to_name
from .util import N_formatter

try:
	from .fuse import ydl_fuse
except:
	ydl_fuse = None

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

def _rename_files(dname, ytid, newname, old_dname=None):
	"""
	Rename all files in directory @dname that contains the youtube ID @ytid into the form
		NEWNAME-YTID.SUFFIX

	If video needs to move directories, then provide @old_dname as the current and @dname as the new directory.
	"""

	# Same base directory
	basedir = os.getcwd()

	# True if any files are moved
	renamed = False

	# If is moving directories, then move it first without changing file name
	# And then (below) rename the files
	if old_dname is not None:
		# Make new directory if it doesn't exist
		# This happens if a single video was added and this is the first video of the uploader
		if not os.path.exists(dname):
			os.makedirs(dname)

		fs = glob.glob("%s/%s/*%s*" % (old_dname, ytid[0], ytid))
		fs2 = glob.glob("%s/%s/.*%s*" % (old_dname, ytid[0], ytid))
		fs = fs + fs2

		for f in fs:
			# Change directory name
			dest = dname + '/' + f.split('/',1)[1]
			print("\t\t%s -> %s" % (f, dest))

			os.rename(f, dest)

	try:
		# Step into sub directory
		os.chdir(basedir + '/' + dname + '/' + ytid[0])

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
			# or "FOO - YTID.caption.en.vtt"
			parts = f.split(ytid)

			# Sometimes this happens that the file downloaded is an MP4 or something
			# and youtube-dl doesn't put a suffix on it (seems to be older videos). Annoying.
			# And it doesn't merge into an mkv as requested. Annoying x2.
			# So this doesn't know what to do with the name, so try to fix the file first and then rename
			if parts[-1] == '':
				# Get file information
				r = subprocess.run(['file', f], stdout=subprocess.PIPE)
				ret = r.stdout.decode('utf-8')
				if 'MP4' in ret:
					dest = f + '.mkv'
					# Assume MP4 and get ffmpeg to convert it
					subprocess.run(['ffmpeg', '-i', f, '-c', 'copy', dest])
					if not os.path.exists(dest):
						raise Exception("Unable to fix this incorrectly downloaded video: YTID=%s, file=%s" % (ytid, f))

					# Remove old file
					os.unlink(f)

					# Redo
					f = dest
					parts = f.split(ytid)
				elif 'Matroska' in ret:
					# This case probably won't happen, but include it anyway as it's easy to handle
					# Just rename
					dest = f + '.mkv'
					os.rename(f, dest)
				else:
					raise Exception("Unknown file contents for %s, `file` output is '%s'" % (f, ret))

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
			elif '.subtitle' in last[0]:
				subparts = last[0].split('subtitle',1)
				suffix = '.subtitle' + subparts[1] + '.' + last[1]
			elif '.caption' in last[0]:
				subparts = last[0].split('caption',1)
				suffix = '.caption' + subparts[1] + '.' + last[1]
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


class YDL:
	"""
	Class to contain the functionality of the stand-alone main part of this library.
	"""

	def __init__(self):
		pass

	def process_args(self):
		"""Process sys.argv and put into self.args"""
		self.args = self._get_args()

		# Do any argument pre-processing here
		if self.args.debug == 'debug':		logging.basicConfig(level=logging.DEBUG)
		elif self.args.debug == 'info':		logging.basicConfig(level=logging.INFO)
		elif self.args.debug == 'warning':	logging.basicConfig(level=logging.WARNING)
		elif self.args.debug == 'error':	logging.basicConfig(level=logging.ERROR)
		elif self.args.debug == 'critical':	logging.basicConfig(level=logging.CRITICAL)
		else:
			raise ValueError("Unrecognized logging level '%s'" % self.args.debug)

		if self.args.notify:
			if not os.path.exists(PUSHOVER_CFG_FILE):
				print("Unable to send notifications because there is no ~/.pushoverrc configuration file")
				print("Aborting.")
				sys.exit(-1)

			if pushover is None:
				print("Unable to send notifications because pushover is not installed: sudo pip3 install pushover")
				print("Aborting.")
				sys.exit(-1)

	def _get_args(self):
		"""Get arguments and return ArgumentParser.parse_args() object"""

		p = argparse.ArgumentParser()
		p.add_argument('-f', '--file', default='ydl.db', help="use sqlite3 FILE (default ydl.db)")
		p.add_argument('--stdin', action='store_true', default=False, help="Accept input on STDIN for parameters instead of arguments")
		p.add_argument('--debug', choices=('debug','info','warning','error','critical'), default='error', help="Set logging level")
		p.add_argument('--rate', nargs=1, default=[900000], type=int, help="Download rate in bps")

		p.add_argument('--add', nargs='*', default=False, help="Add URL(s) to download")
		p.add_argument('--name', nargs='*', default=False, help="Supply a YTID and file name to manually specify it")
		p.add_argument('--alias', nargs='*', default=False, help="Add an alias for unnamed channels")
		p.add_argument('--list', nargs='*', default=False, help="List of lists")
		p.add_argument('--listall', nargs='*', default=False, help="Same as --list but will list all the videos too")
		p.add_argument('--showpath', nargs='*', default=False, help="Show file paths for the given channels or YTID's")
		p.add_argument('--skip', nargs='*', help="Skip the specified videos (supply no ids to get a list of skipped). If video is marked sleep, it will be removed from that list and marked skip.")
		p.add_argument('--unskip', nargs='*', help="Un-skip the specified videos (supply no ids to get a list of not skipped)")
		p.add_argument('--sleep', nargs='*', help="Sleep the specified video until the time in UTC (YYYY-MM-DD HH:MM:SS format)")
		p.add_argument('--unsleep', nargs='*', help="Remove the specified videos from the sleep list")
		p.add_argument('--noautosleep', action='store_true', default=False, help="If video indicates it premiers in the future, it will automatically be added to the sleep list. Pass this to disable this. Default is to auto-sleep.")
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

		p.add_argument('--merge-playlist', default=False, nargs='+', help="Merge a playlist into a single video file with each video entry as a chapter")

		p.add_argument('--chapter-edit', default=False, nargs=1, help="Edit chapters for the given video file")
		p.add_argument('--chapterize', default=False, nargs='+', help="Add chapters to a file. Must use --chapter-edit first to provide chapter information, then --chapterize the video.")
		p.add_argument('--split', default=False, nargs=3, help="Split video into the specified output file type (eg, mkv for video, mp3:128kbps, mp3:320kbps, ogg:8.0) and the output string format (can use standard python string formatting with {artist}, {album}, {N}, {total}, {year}, {genre}, {ytid}, {name}). Must use --chapter-edit first to provide the chapter information, then --split the video. Use --convert to do the entire video into a single file.")
		p.add_argument('--convert', default=False, nargs=3, help="Convert video into the specified output file type (eg, mp3:128kbps, ogg:8.0) and the output string format (can use standard python string formatting with {artist}, {album}, {N}, {total}, {year}, {genre}, {ytid}, {name}). Use --split to dice up into multiple files, or --convert for the entire video in one.")

		p.add_argument('--artist', default=False, help="Set artist, if splitting to audio file")
		p.add_argument('--album', default=False, help="Set album, if splitting to audio file")
		p.add_argument('--year', default=False, help="Set year, if splitting to audio file")
		p.add_argument('--genre', default=False, help="Set genre, if splitting to audio file")
		p.add_argument('--format-name', default=False, help="Format the name string (eg, '{N} {name}, if splitting to audio file")
		p.add_argument('--caption-language', default="en", help="Specify the caption language to download. Comma-delimited if multiple. Empty string if all.")

		#TODO: pull caption-language default from environmental variables (LANG, LANGUAGE)

		return p.parse_args()

	def open_db(self):
		"""Open the database object"""
		self.db = ydl.db(os.getcwd() + '/' + self.args.file)
		self.db.open()

		# Do any verification of the database here

	def main(self):
		""" Main function called from invoking the library """

		self.process_args()

		self.open_db()

		if self.args.fuse:
			self.fuse()
			sys.exit()

		if type(self.args.add) is list:
			self.add()

		if self.args.skip is not None:
			self.skip()

		if self.args.unskip is not None:
			self.unskip()

		if self.args.sleep is not None:
			self.sleep()

		if self.args.unsleep is not None:
			self.unsleep()

		if type(self.args.name) is list:
			self.name()

		if type(self.args.alias) is list:
			self.alias()

		if self.args.update_names is not False:
			self.updatenames()

		if type(self.args.info) is list:
			self.info()

		if type(self.args.showpath) is list:
			self.showpath()

		if type(self.args.list) is list or type(self.args.listall) is list:
			self.list()

		if self.args.sync is not False or self.args.sync_list is not False:
			self.sync_list()

		if self.args.sync is not False or self.args.sync_videos is not False:
			self.sync_videos()

		if self.args.download is not False:
			self.download()

		if self.args.merge_playlist is not False:
			self.merge_playlist()

		if self.args.chapter_edit is not False:
			self.chapter_edit()

		if self.args.chapterize is not False:
			self.chapterize()

		if self.args.split is not False:
			self.split()

	def add(self):
		# Processing list of URLs
		urls = []

		if self.args.stdin:
			vals = [_.strip() for _ in sys.stdin.readlines()]
		else:
			vals = self.args.add

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
				if len(q) != 3 and (len(q) == 4 and q[-1] != ''):
					print(url)
					print("\t" + "User URL expected to have a name after /user/")
					sys.exit(-1)
				urls.append( ('u', q[2]) )

			if u.path.startswith('/c/'):
				q = u.path.split('/')
				if len(q) != 3 and (len(q) == 4 and q[-1] != ''):
					print(url)
					print("\t" + "Channel URL expected to have a channel name after /c/")
					sys.exit(-1)
				urls.append( ('c', q[2]) )

			if u.path.startswith('/channel/'):
				q = u.path.split('/')
				if len(q) != 3 and (len(q) == 4 and q[-1] != ''):
					print(url)
					print("\t" + "Channel URL expected to have a channel name after /channel/")
					sys.exit(-1)
				urls.append( ('ch', q[2]) )

			# FIXME: if something like https://youtube.com/foo passed it is silently ignored
			# need to catch un-matched URLs and error (ie, terminal else clause here)

		self.db.begin()

		for i,u in enumerate(urls):
			print("%d of %d: %s" % (i+1, len(urls), u[1]))

			if u[0] == 'v':
				o = self.db.get_video(u[1])
				if o:
					print("\tFound")
				else:
					print("\tNot found")
					self.db.add_video(u[1], "MISCELLANEOUS")
					print("\tAdded")

			elif u[0] == 'u':
				o = self.db.get_user(u[1])
				if o:
					print("\tFound")
				else:
					print("\tNot found")
					self.db.add_user(u[1])
					if not os.path.exists(u[1]):
						os.mkdir(u[1])
					print("\tAdded")

			elif u[0] == 'p':
				o = self.db.get_playlist(u[1])
				if o:
					print("\tFound")
				else:
					print("\tNot found")
					self.db.add_playlist(u[1])
					if not os.path.exists(u[1]):
						os.mkdir(u[1])
					print("\tAdded")

			elif u[0] == 'c':
				o = self.db.get_channel_named(u[1])
				if o:
					print("\tFound")
				else:
					print("\tNot found")
					self.db.add_channel_named(u[1])
					os.mkdir(u[1])
					print("\tAdded")

			elif u[0] == 'ch':
				o = self.db.get_channel_unnamed(u[1])
				if o:
					print("\tFound")
				else:
					print("\tNot found")
					self.db.add_channel_unnamed(u[1])
					if not os.path.exists(u[1]):
						os.mkdir(u[1])
					print("\tAdded")

			else:
				raise ValueError("Unrecognize URL type %s" % (u,))

		self.db.commit()

	def info(self):
		if not len(self.args.info):
			self.info_db()
		else:
			self.info_videos()

	def info_db(self):
		# Prune any sleeping videos
		pruned = self._prunesleep()

		cs = self.db.c.num_rows()
		chs = self.db.ch.num_rows()
		us = self.db.u.num_rows()
		pls = self.db.pl.num_rows()

		print("Database information")
		print("\tFile: %s" % self.db.Filename)
		print()
		print("\tNamed channels: %d" % cs)
		print("\tUnnamed channels: %d" % chs)
		print("\tUsers: %d" % us)
		print("\tPlaylists: %d" % pls)

		total = vs = self.db.v.num_rows()
		print("\tVideos: %d" % vs)
		vs = self.db.v.num_rows('`skip`=1')
		print("\t\tSkipped: %d" % vs)
		vs = self.db.v.num_rows('`utime` is not null')
		print("\t\tDownloaded: %d (%.2f%%)" % (vs,100*vs/total))
		vs = self.db.vnames.num_rows()
		print("\t\tWith preferred names: %d" % vs)
		vs = self.db.v_sleep.num_rows()
		print("\t\tSleeping: %d" % vs)
		print("\t\tSleeping just pruned: %d" % len(pruned))

		row = self.db.execute("select sum(duration) as duration from v").fetchone()
		days = row['duration'] / (60*60*24.0)
		print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))

		print("Calculating disk space used...")

		args = ['du', '-b', '-s', os.path.dirname(self.db.Filename)]
		s = subprocess.run(args, stdout=subprocess.PIPE)
		line = s.stdout.decode('ascii').split()
		sz = int(line[0])
		print("\t%d bytes (%s)" % (sz, bytes_to_str(sz)))

	def info_videos(self):
		ytids = self.args.info
		print("Showing information for videos (%d):" % len(ytids))

		# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
		ytids = ['-' + _[1:] for _ in ytids if _[0] == '='] + [_ for _ in ytids if _[0] != '=']

		for ytid in ytids:
			row = self.db.v.select_one('*', '`ytid`=?', [ytid])
			if row is not None:
				self.info_v(ytid, row)
				continue

			# Check if named channel
			row = self.db.c.select_one('*', '`name`=?', [ytid])
			if row is not None:
				print("\tNamed channel %s:" % ytid)
				rows = self.db.v.select('*', '`dname`=?', [ytid])
				rows = [dict(_) for _ in rows]
				rows = sorted(rows, key=lambda x: x['ytid'])

				row = self.db.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
				days = row['duration'] / (60*60*24.0)
				print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
				print()

				for row in rows:
					self.info_v(row['ytid'], row)

				# Don't, next @ytids entry
				continue

			# Check if unnamed channel
			row = self.db.ch.select_one('*', '`name`=? or `alias`=?', [ytid,ytid])
			if row is not None:
				print("\tUnnamed channel %s:" % ytid)
				rows = self.db.v.select('*', '`dname`=?', [ytid])
				rows = [dict(_) for _ in rows]
				rows = sorted(rows, key=lambda x: x['ytid'])

				row = self.db.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
				days = row['duration'] / (60*60*24.0)
				print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
				print()

				for row in rows:
					self.info_v(row['ytid'], row)

				# Don't, next @ytids entry
				continue

			# Check if user
			row = self.db.u.select_one('*', '`name`=?', [ytid])
			if row is not None:
				print("\tUser %s:" % ytid)
				rows = self.db.v.select('*', '`dname`=?', [ytid])
				rows = [dict(_) for _ in rows]
				rows = sorted(rows, key=lambda x: x['ytid'])

				row = self.db.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
				days = row['duration'] / (60*60*24.0)
				print("\t\tTotal duration: %s (%.2f days)" % (sec_str(row['duration']), days))
				print()

				for row in rows:
					self.info_v(row['ytid'], row)

				# Don't, next @ytids entry
				continue

			# Check if playlist
			row = self.db.pl.select_one('*', '`ytid`=?', [ytid])
			if row is not None:
				print("\tPlaylist %s:" % ytid)
				rows = self.db.v.select('*', '`dname`=?', [ytid])
				rows = [dict(_) for _ in rows]
				rows = sorted(rows, key=lambda x: x['ytid'])

				row = self.db.execute("select sum(duration) as duration from v where `dname`=?", (ytid,)).fetchone()
				if row['duration'] is None:
					duration = 0
					days = 0.0
				else:
					duration = row['duration']
					days = duration / (60*60*24.0)

				print("\t\tTotal duration: %s (%.2f days)" % (sec_str(duration), days))
				print()

				for row in rows:
					self.info_v(row['ytid'], row)

				# Don't, next @ytids entry
				continue

			print("\t%s -- NOT FOUND" % ytid)

	def info_v(self, ytid, row):
		pruned = self._prunesleep()

		row = self.db.v.select_one('*', '`ytid`=?', [ytid])
		if row is None:
			print("\t\tNot found")
			return

		path = ydl.db.format_v_fname(row['dname'], row['name'], None, ytid, 'mkv')
		exists = os.path.exists(path)
		size = None
		if exists:
			size = os.stat(path).st_size
			size = '%s (%d bytes)' % (bytes_to_str(size), size)
		else:
			size = ''

		row_sleep = self.db.v_sleep.select_one('t', 'ytid=?', [ytid])
		if row_sleep is None:
			sleep = False
		else:
			now = datetime.datetime.utcnow()
			delta = row_sleep['t'] - now
			sleep = "%s (until %s UTC, %s away)" % (True, row_sleep['t'].strftime("%Y-%m-%d %H:%M:%S"), delta)

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
			['Sleeping?', sleep],
			['Path', path],
			['Exists?', exists],
			['Size', size],
		]
		if row['chapters'] is not None:
			p = os.getcwd() + '/CHAPTERIZED/' + row['ytid'] + '.chapters.mkv'

			s = os.getcwd() + '/SPLIT/' + row['ytid'] + '/'

			inf += [
				['Has Chapter Info?', True],
				['Chapterize File', p],
				['Chapterize File Exists?', os.path.exists(p)],
				['Split Directory', s],
				['Split Directory Exists?', os.path.exists(s)],
			]
		else:
			inf += [
				['Has Chapter Info?', False],
			]

		print_2col(inf)

	def _show_skip(self):
		res = self.db.v.select(["ytid","dname","name"], "`skip`=?", [True])
		ytids = [dict(_) for _ in res]
		ytids = sorted(ytids, key=lambda _:_['dname']+'/'+_['ytid'])

		if self.args.json:
			print(json.dumps(ytids))
		elif self.args.xml:
			raise NotImplementedError("XML not implemented yet")
		else:
			# FIXME: abide by --json and --xml
			print("Videos marked skip (%d):" % len(ytids))
			for row in ytids:
				if row['name'] is None:
					print("\t%s -- %s/%s" % (row['ytid'], row['dname'], '?'))
				else:
					print("\t%s -- %s/%s" % (row['ytid'], row['dname'], row['name']))

		res = self.db.pl.select("ytid", "`skip`=?", [True])
		ytids = [_['ytid'] for _ in res]
		ytids = sorted(ytids)

		if self.args.json:
			print(json.dumps(ytids))
		elif self.args.xml:
			raise NotImplementedError("XML not implemented yet")
		else:
			# FIXME: abide by --json and --xml
			print("Playlists marked skip (%d):" % len(ytids))
			for ytid in ytids:
				print("\t%s" % ytid)

	def skip(self):
		"""
		List or add videos to the skip list.
		"""

		if not len(self.args.skip):
			self._show_skip()
		else:
			# This could signify STDIN contains json or xml to intrepret as ytids???
			if self.args.json:
				raise NotImplementedError("--json not meaningful when adding skipped videos")
			if self.args.xml:
				raise NotImplementedError("--xml not meaningful when adding skipped videos")

			ytids = list(set(self.args.skip))
			ytids = ['-' + _[1:] for _ in ytids if _[0] == '='] + [_ for _ in ytids if _[0] != '=']

			# Split into videos and playlists
			v_ytids = [_ for _ in ytids if len(_) == 11]
			pl_ytids= [_ for _ in ytids if len(_) != 11]


			self.db.begin()
			print("Marking videos to skip (%d):" % len(v_ytids))
			for ytid in v_ytids:
				print("\t%s" % ytid)
				row = self.db.v.select_one("rowid", "`ytid`=?", [ytid])
				self.db.v.update({"rowid": row['rowid']}, {"skip": True})

				# Delete any sleep times for this video, this will not error if no rows present
				self.db.v_sleep.delete({'ytid': ytid})

			print("Marking playlists to skip (%d):" % len(pl_ytids))
			for ytid in pl_ytids:
				print("\t%s" % ytid)
				row = self.db.pl.select_one("rowid", "`ytid`=?", [ytid])
				self.db.pl.update({"rowid": row['rowid']}, {"skip": True})
			self.db.commit()

	def unskip(self):
		"""
		Remove videos from the skip list
		"""

		if not len(self.args.unskip):
			self._show_skip()
		else:
			# This could signify STDIN contains json or xml to intrepret as ytids???
			if self.args.json:
				raise NotImplementedError("--json not meaningful when removing skipped videos")
			if self.args.xml:
				raise NotImplementedError("--xml not meaningful when removed skipped videos")

			ytids = list(set(self.args.unskip))
			print('ytids', ytids)

			# Split into videos and playlists
			v_ytids = [_ for _ in ytids if len(_) == 11]
			pl_ytids= [_ for _ in ytids if len(_) != 11]

			self.db.begin()
			print("Marking videos to not skip (%d):" % len(v_ytids))
			for ytid in v_ytids:
				print("\t%s" % ytids)
				row = self.db.v.select_one("rowid", "`ytid`=?", [ytid])
				self.db.v.update({"rowid": row['rowid']}, {"skip": False})

			print("Marking playlists to not skip (%d):" % len(pl_ytids))
			for ytid in pl_ytids:
				print("\t%s" % ytids)
				row = self.db.pl.select_one("rowid", "`ytid`=?", [ytid])
				self.db.pl.update({"rowid": row['rowid']}, {"skip": False})
			self.db.commit()


	def _prunesleep(self):
		"""
		Internal function to check sleep tables for things to prune.
		"""
		fmt = "%Y-%m-%d %H:%M:%S"

		res = self.db.v_sleep.select(['rowid','ytid','t'], order="t asc")
		rows = [dict(_) for _ in res]

		now = datetime.datetime.utcnow()

		prune = []
		for row in rows:
			# Next row, and all subsequent rows, are after now and won't be pruned
			if row['t'] > now:
				break

			prune.append(row)

		if len(prune):
			print("Auto-pruning sleep times and will be available immediately for actions:")

			self.db.begin()
			for row in prune:
				print("\t%s -- %s" % (row['ytid'], row['t'].strftime(fmt)))
				self.db.v_sleep.delete({'rowid': row['rowid']})
			self.db.commit()

			print("")
			print("")

		return [_['ytid'] for _ in prune]

	def sleep(self):
		"""
		Marks videos to sleep until the given date.
		"""

		fmt = "%Y-%m-%d %H:%M:%S"

		pruned = self._prunesleep()

		if len(self.args.sleep) == 0:
			res = self.db.v_sleep.select("*", order="t asc")
			rows = [dict(_) for _ in res]

			now = datetime.datetime.utcnow()
			print("%d on the sleep list" % len(rows))
			for row in rows:
				delta = row['t'] - now
				print("\t%12s -- %s (%s away)" % (row['ytid'], row['t'].strftime(fmt), delta))

		elif len(self.args.sleep) == 1:
			ytid = self.args.sleep[0]
			if ytid[0] == '=':
				ytid = '-' + ytid[1:]

			now = datetime.datetime.utcnow()

			print("Checking sleep list: %s" % ytid)
			if ytid in pruned:
				print("\tVideo exceeded sleep time and was pruned")
			else:
				print("\tCurrent: %s (UTC)" % now.strftime(fmt))

				row = self.db.v_sleep.select_one(['rowid','t'], "`ytid`=?", [self.args.sleep[0]])
				if row is None:
					print("\tNOT LISTED")
				else:
					print("\tSleep: %s (UTC)" % row['t'].strftime(fmt))
					print("\tSleep remaining: %s" % (row['t'] - now,))

		elif len(self.args.sleep) == 2:
			ytid = self.args.sleep[0]
			if ytid[0] == '=':
				ytid = '-' + ytid[1:]

			t = self.args.sleep[1]

			if '+' in t:
				# Specifying a relative date (eg, "d+10" for 10 days from now, "h+10" for 10 hours from now)

				if t[0] == 'd':
					# Current time plus X days
					t = datetime.datetime.utcnow() + datetime.timedelta(days=int(t[2:]))
				elif t[0] == 'h':
					# Current time plus X hours
					t = datetime.datetime.utcnow() + datetime.timedelta(hours=int(t[2:]))
				elif t[0] == 'm':
					# Current time plus X minutes
					t = datetime.datetime.utcnow() + datetime.timedelta(minutes=int(t[2:]))
				elif t[0] == 's':
					# Current time plus X seconds
					t = datetime.datetime.utcnow() + datetime.timedelta(seconds=int(t[2:]))
				else:
					print("Unrecognized relative time format: %s" % t)
			else:
				# Absolute time format
				t = datetime.datetime.strptime(t, fmt)

			print("Adding to the sleep list: %s" % ytid)
			print("\tCurrent: %s (UTC)" % datetime.datetime.utcnow().strftime(fmt))
			print("\tSleep: %s (UTC)" % t.strftime(fmt))

			if ytid in pruned:
				print("\tVideo exceeded sleep time and was pruned, but re-added it back")

			row = self.db.v_sleep.select_one(['rowid','t'], "`ytid`=?", [self.args.sleep[0]])
			if row is not None:
				print("\tAlready listed to sleep until %s, will alter sleep time" % row['t'].strftime(fmt))

				# Update the time
				self.db.begin()
				self.db.v_sleep.update({'rowid': row['rowid']}, {'t': t})
				self.db.commit()
			else:
				# Insert the time
				self.db.begin()
				self.db.v_sleep.insert(ytid=ytid, t=t)
				self.db.commit()

	def unsleep(self):
		"""
		Remove videos from sleep list.
		Any videos provided that aren't marked for sleeping will be silently ignored.
		"""
		pruned = self._prunesleep()

		print("Removing the following videos from the sleep list:")

		if len(self.args.unsleep) == 0:
			print("")
		elif len(self.args.unsleep) == 1 and self.args.unsleep == '*':
			res = self.db.v_sleep.select(['rowid','ytid','t'])
			rows = [dict(_) for _ in res]

			self.db.begin()
			for row in rows:
				print("\t%s" % row['ytid'])
				self.db.v_sleep.delete({'rowid': row['rowid']})

			self.db.commit()

		else:
			self.db.begin()
			for ytid in self.args.unsleep:
				if ytid[0] == '=':
					ytid = '-' + ytid[1:]

				if ytid in pruned:
					print("\t%s (auto-pruned above)" % ytid)
				else:
					print("\t%s" % ytid)
					self.db.v_sleep.delete({'ytid': ytid})

			self.db.commit()

	def name(self):
		"""
		List all the preferred names if --name.
		List information about a single video if --name YTID is provided.
		Set preferred name if --name YTID NAME is provided
		"""

		if len(self.args.name) == 0:
			res = self.db.vnames.select(['ytid','name'])
			rows = [dict(_) for _ in res]
			rows = sorted(rows, key=lambda x: x['ytid'])

			print("Preferred names (%d):" % len(rows))
			for row in rows:
				sub_row = self.db.v.select_one('dname', '`ytid`=?', [row['ytid']])

				print("\t%s -> %s / %s" % (row['ytid'], sub_row['dname'], row['name']))

		elif len(self.args.name) == 1:
			ytid = self.args.name[0]

			row = self.db.v.select_one(['rowid','dname','name','title'], '`ytid`=?', [ytid])
			if not row:
				print("No video with YTID '%s' found" % ytid)
				sys.exit()

			print("YTID: %s" % ytid)
			print("Title: %s" % row['title'])
			print("Directory: %s" % row['dname'])
			print("Computed name: %s" % row['name'])

			row = self.db.vnames.select_one('name', '`ytid`=?', [ytid])
			if row:
				print("Preferred name: %s" % row['name'])
			else:
				print("-- NO PREFERRED NAME SET --")

		elif len(self.args.name) == 2:
			ytid = self.args.name[0]

			pref_name = ydl.db.title_to_name(self.args.name[1])
			if pref_name != self.args.name[1]:
				raise KeyError("Name '%s' is not valid" % self.args.name[1])

			dname = self.db.get_v_dname(ytid, absolute=False)

			# Get file name without suffix
			fname = self.db.get_v_fname(ytid, suffix=None)

			# Rename old files
			_rename_files(dname, ytid, pref_name)

			self.db.begin()
			row = self.db.vnames.select_one('rowid', '`ytid`=?', [ytid])
			if row:
				self.db.vnames.update({'rowid': row['rowid']}, {'name': pref_name})
			else:
				self.db.vnames.insert(ytid=ytid, name=pref_name)
			self.db.commit()

		else:
			print("Too many arguments")

	def alias(self):
		if len(self.args.alias) == 0:
			res = self.db.ch.select(['rowid','name','alias'])
			rows = [dict(_) for _ in res]
			print("Existing channels:")
			for row in rows:
				if row['alias'] is None:
					print("\t%s" % row['name'])
				else:
					print("\t%s -> %s" % (row['name'], row['alias']))
		elif len(self.args.alias) == 1:
			row = self.db.ch.select_one(['name','alias'], "`name`=? or `alias`=?", [args.alias[0], args.alias[0]])
			print("Channel: %s" % row['name'])
			print("Alias: %s" % row['alias'])

		elif len(self.args.alias) == 2:
			res = self.db.ch.select('*', '`name`=?', [self.args.alias[1]])
			rows = [dict(_) for _ in res]
			if len(rows):
				raise ValueError("Alias name already used for an unnamed channel: %s" % rows[0]['name'])

			res = self.db.ch.select('*', '`alias`=?', [self.args.alias[1]])
			rows = [dict(_) for _ in res]
			if len(rows):
				if rows[0]['name'] == self.args.alias[0]:
					# Renaming to same alias
					sys.exit()
				else:
					raise ValueError("Alias name already used for an unnamed channel: %s" % rows[0]['name'])

			res = self.db.c.select('*', '`name`=?', [self.args.alias[1]])
			rows = [dict(_) for _ in res]
			if len(rows):
				raise ValueError("Alias name already used for an named channel: %s" % rows[0]['name'])

			res = self.db.u.select('*', '`name`=?', [self.args.alias[1]])
			rows = [dict(_) for _ in res]
			if len(rows):
				raise ValueError("Alias name already used for a user: %s" % rows[0]['name'])


			# FIXME: changing alias to a second alias doesn't fix v.dname, but does fix ch.alias and the directory name

			pref = ydl.db.alias_coerce(self.args.alias[1])
			if pref != self.args.alias[1]:
				raise KeyError("Alias '%s' is not valid" % self.args.name[1])

			row = self.db.ch.select_one(['rowid','alias'], '`name`=?', [self.args.alias[0]])
			if row is None:
				raise ValueError("No channel by %s" % self.args.alias[0])

			# Used for updating vids table
			old_name = self.args.alias[0]


			# Old and new directory names
			old = os.getcwd() + '/' + self.args.alias[0]
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
			self.db.begin()
			self.db.ch.update({'rowid': row['rowid']}, {'alias': pref})
			self.db.v.update({'dname': self.args.alias[0]}, {'dname': pref})
			self.db.vids.update({'name': old_name}, {'name': pref})
			self.db.commit()

		else:
			print("Too many variables")

	def list(self):
		"""
		List all the user, unnamed channels, named channels, and playlists.
		If --listall supplied then list all of that and the videos for each list.
		"""

		self._list(self.db.u, 'name')
		self._list(self.db.c, 'name')
		self._list(self.db.ch, 'name')
		self._list(self.db.ch, 'alias')
		self._list(self.db.pl, 'ytid')

	def listall(self, ytids):
		"""
		List the videos for the YTID's provided in @ytids.
		"""

		pruned = self._prunesleep()


		# Count number of videos that exist
		counts = 0
		skipped = 0
		sleeping = 0

		ytids_str = list_to_quoted_csv(ytids)

		# Get video data for all the videos supplied
		# I don't know if there's a query length limit...
		res = self.db.v.select(["ytid","dname","name","title","duration","skip"], "`ytid` in (%s)" % ytids_str)
		rows = {_['ytid']:_ for _ in res}

		# Map ytids to alias
		res = self.db.vnames.select(["ytid","name"], "`ytid` in (%s)" % ytids_str)
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

			row_sleep = self.db.v_sleep.select_one('t', 'ytid=?', [ytid])
			if row_sleep is not None:
				now = datetime.datetime.utcnow()
				delta = row_sleep['t'] - now
				print("\t\t%s: L (until %s UTC, %s away)" % (ytid, row_sleep['t'], delta))
				sleeping += 1
				continue

			# Check if there's an alias, otherwise format_v_fname takes None for the value
			alias = None
			if ytid in aliases:
				alias = aliases[ytid]

			# All DB querying is done above, so just format it
			path = ydl.db.format_v_fname(row['dname'], row['name'], alias, ytid, "mkv")

			# Check if it exists
			exists = os.path.exists(path)
			if exists:
				counts += 1

			if row['title'] is None:
				print("\t\t%s: ?" % ytid)
			else:
				t = row['title']
				t = t.replace('\n', '\\n')

				if row['duration'] is None:
					if exists:
						print("\t\t%s: E %s" % (ytid, t))
					else:
						print("\t\t%s:   %s" % (ytid, t))

				else:
					if exists:
						print("\t\t%s: E %s (%s)" % (ytid, t, sec_str(row['duration'])))
					else:
						print("\t\t%s:   %s (%s)" % (ytid, t, sec_str(row['duration'])))

		print()
		print("\t\tSkipped (S): %d of %d" % (skipped, len(ytids)))
		print("\t\tSleeping (L): %d of %d" % (sleeping, len(ytids)))
		print("\t\tExists (E): %d of %d non-skipped, non-sleeping" % (counts, len(ytids)-skipped-sleeping))

	def _list(self, sub_d, col_name):
		where = ""
		if type(self.args.list) is list and len(self.args.list):
			where = "`%s` in (%s)" % (col_name, list_to_quoted_csv(self.args.list))
		if type(self.args.listall) is list and len(self.args.listall):
			where = "`%s` in (%s)" % (col_name, list_to_quoted_csv(self.args.listall))

		res = sub_d.select("*", where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _[col_name])


		print("%s (%d):" % (sub_d.Name, len(rows)))
		for row in rows:
			sub_res = self.db.vids.select(["rowid","ytid"], "`name`=?", [row[col_name]], "`idx` asc")
			sub_rows = [dict(_) for _ in sub_res]
			sub_cnt = len(sub_rows)

			print("\t%s (%d)" % (row[col_name], sub_cnt))

			# Do only if --listall
			if type(self.args.listall) is list:
				ytids = [_['ytid'] for _ in sub_rows]
				self.listall(ytids)

	def showpath(self):
		"""
		Show paths of all the videos
		"""

		if not len(self.args.showpath):
			raise KeyError("Must provide a channel to list, use --list to get a list of them")

		where = "(`ytid` in ({0}) or `dname` in ({0}))".format(list_to_quoted_csv(self.args.showpath))

		res = self.db.v.select(['rowid','ytid','dname','name','title','duration'], where)
		rows = [dict(_) for _ in res]
		rows = sorted(rows, key=lambda _: _['ytid'])

		for row in rows:
			path = self.db.get_v_fname(row['ytid'])

			exists = os.path.exists(path)
			if exists:
				print("%s: E %s (%s)" % (row['ytid'],row['title'],sec_str(row['duration'])))
			else:
				print("%s:   %s (%s)" % (row['ytid'],row['title'],sec_str(row['duration'])))

			print("\t%s" % path)
			print()

	def sync_list(self):
		pruned = self._prunesleep()

		filt = None
		if type(self.args.sync) is list:		filt = self.args.sync
		if type(self.args.sync_list) is list:	filt = self.args.sync_list

		z = [
			(self.db.u, 'name', ydl.get_list_user),
			(self.db.c, 'name', ydl.get_list_c),
			(self.db.ch, 'name', ydl.get_list_channel),
			(self.db.pl, 'ytid', ydl.get_list_playlist),
		]

		for d_sub, col_name, ydl_func in z:
			print("Update %s" % d_sub.Name)

			rss_ok = not self.args.no_rss

			# Not applicable to playlists (no RSS)
			if d_sub.Name == 'pl':
				rss_ok = False

			_sync_list(self.args, self.db, d_sub, filt, col_name, self.args.ignore_old, rss_ok, ydl_func)

	def sync_videos(self):
		"""
		Sync all videos in the database @d and if @ignore_old is True then don't sync
		those videos that have been sync'ed before.
		"""

		pruned = self._prunesleep()

		filt = None
		if type(self.args.sync) is list:			filt = self.args.sync
		if type(self.args.sync_videos) is list:		filt = self.args.sync_videos

		# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
		filt = ['-' + _[1:] for _ in filt if _[0] == '='] + [_ for _ in filt if _[0] != '=']

		print("Sync all videos")
		# Get videos
		res = self.db.get_v(filt, self.args.ignore_old)
		rows = [_ for _ in res]

		# Convert rows to dictionaries
		rows = [dict(_) for _ in rows]
		# Sort by YTID to be consistent
		rows = sorted(rows, key=lambda x: x['ytid'])

		summary = {
			'done': [],
			'error': [],
			'paymentreq': [],
			'skip': [],
		}

		try:
			# Iterate over videos
			for i,row in enumerate(rows):
				# print to the screen to show progress
				print("\t%d of %d: %s" % (i+1,len(rows), row['ytid']))
				self.sync_video(row, summary)

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

	def sync_video(self, row, summary):
		ytid = row['ytid']
		rowid = row['rowid']
		ctime = row['ctime']
		skip = row['skip']

		# If instructed to skip, then skip
		# This can be done if the video is on a playlist, etc that is not available to download
		if skip:
			print("\t\tSkipping")
			# This marks it as at least looked at, otherwise repeated --sync --ignore-old will keep checking
			self.db.v.update({"rowid": rowid}, {"atime": _now()})
			return None

		# Get video information
		try:
			ret = ydl.get_info_video(ytid)
		except KeyboardInterrupt:
			# Pass it down
			raise
		except ydl.PaymentRequiredException:
			summary['paymentreq'].append(ytid)
			return None
		except Exception as e:
			traceback.print_exc()
			summary['error'].append(ytid)
			return None

		# Squash non-ASCII characters (I don't like emoji in file names)
		name = ydl.db.title_to_name(ret['title'])

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
		self.db.begin()
		self.db.v.update({'rowid': rowid}, dat)
		self.db.commit()

		# Got it
		summary['done'].append(ytid)

	def fuse(self):
		pruned = self._prunesleep()

		# Get mount point
		mnt = self.args.fuse[0]

		# Absolute path it
		mnt = os.path.abspath(mnt)

		# Determine what to prepend to the symlink paths
		if self.args.fuse_absolute:
			rootbase = os.path.abspath( os.path.dirname(self.db.Filename) )
		else:
			# Get absolute path of the YDL database
			fpath = os.path.abspath(self.args.file)
			# Get the directory that file is in
			fpath = os.path.dirname(fpath)

			# Get the absolute path of the mount point
			root = os.path.abspath(self.args.fuse[0])

			# Get the relative path from the
			rootbase = os.path.relpath(fpath, root)

		if not os.path.exists(mnt):
			print("Path %s does not exist" % mnt)
			sys.exit(-1)

		s = os.stat(mnt)
		if not stat.S_ISDIR(s.st_mode):
			print("Path %s is not a directory" % mnt)
			sys.exit(-1)

		print("Mounting YDL database as a FUSE filesystem")
		print("\tDB: %s" % os.path.abspath(self.args.file))
		print("\tMount: %s" % os.path.abspath(mnt))
		print("Enter ctrl-c to quit and unmount")
		print("Mounting...")
		ydl_fuse(self.db, mnt, rootbase, allow_other=True)

	def merge_playlist(self):
		pruned = self._prunesleep()

		dname = os.path.dirname(self.db.Filename) + '/MERGED'
		if not os.path.exists(dname):
			os.makedirs(dname)

		print("Checking that all provided playlists have been completely downloaded first")
		print()

		dat = {}

		abort = False
		for ytid in self.args.merge_playlist:
			print(ytid)
			dat[ytid] = []

			row = self.db.pl.select_one('*', '`ytid`=?', [ytid])
			if row is None:
				print("\tPlaylist %s not found" % ytid)
				abort = True
			else:
				fname = dname + '/' + ytid + '.chapters.mkv'
				if os.path.exists(fname):
					print("\tEXISTS, skipping")
				else:
					# TODO: suspect there will be an issue with skipped videos, will skip (*snark*) that check for now

					# Total length of playlist in seconds
					totallen = 0

					res = self.db.vids.select('*', '`name`=?', [ytid])
					vids = [dict(_) for _ in res]
					vids = sorted(vids, key=lambda _: _['idx'])

					cnt = 0
					for v in vids:
						row = self.db.v.select_one(['duration','title'], '`ytid`=?', [v['ytid']])

						path = self.db.get_v_fname(v['ytid'])
						exists = os.path.exists(path)
						if not exists:
							print("\t%d: %s - DOES NOT EXIST" % (v['idx'], v['ytid']))
						else:
							print("\t%d: %s (%s) - EXISTS" % (v['idx'],v['ytid'],  sec_str(row['duration'])))
							cnt += 1

							dat[ytid].append({'ytid': v['ytid'], 'start': totallen, 'path': path, 'title': row['title']})

							totallen += row['duration']

					print()
					print("%d of %d exists" % (cnt, len(vids)))
					if cnt != len(vids):
						print("Not all videos are downloaded, cannot merge. Do --download first.")
						abort = True
					else:
						print("Expected video length: %s" % sec_str(totallen))

		if abort:
			sys.exit(-1)

		print()
		print('-'*80)
		print("Processing playlists and merging")
		print()

		for ytid in self.args.merge_playlist:
			print(ytid)

			basedir = os.getcwd()
			try:
				os.chdir(basedir + '/MERGED')
				fname_mkv = ytid + '.mkv'
				fname_list = ytid + '.txt'
				fname_chaps = ytid + '.chapters.xml'
				fname_chapsmkv = ytid + '.chapters.mkv'

				if os.path.exists(fname_chapsmkv):
					print("\tAlready merged, skipping")
					continue


				if not os.path.exists(fname_list):
					# Write list of videos to merge
					with open(fname_list, 'w') as f:
						for v in dat[ytid]:
							f.write("file '%s'\n" % v['path'])

				if not os.path.exists(fname_mkv):
					# Merge videos
					# NB: this currently transcodes to h265 as the VP9 codec has issues with invisible frames
					args = ['ffmpeg', '-f', 'concat', '-safe', '0', '-i', fname_list, '-c:v', 'h264', '-c:a', 'copy', fname_mkv]
					print(" ".join(args))
					subprocess.run(args)

				if not os.path.exists(fname_chaps):
					# Create chapters XML
					cxml = mkvxmlmaker.MKVXML_chapter()
					for v in dat[ytid]:
						cxml.AddChapter(sec_str(v['start']), v['title'])
					cxml.Save(fname_chaps)

				# Add in chapter info
				args = ['mkvmerge', '-o', fname_chapsmkv, '--chapters', fname_chaps, fname_mkv]
				print(" ".join(args))
				subprocess.run(args)

			finally:
				os.chdir(basedir)
			print()
			pass

		print()
		print("-"*80)
		print("Final file names")
		print()

		for ytid in self.args.merge_playlist:
			fname_chapsmkv = ytid + '.chapters.mkv'

			print(ytid)
			print("\tMERGED/%s" % fname_chapsmkv)

	def chapter_edit(self):
		pruned = self._prunesleep()

		abort = False

		dat = {}

		print("Checking that videos are present")
		print()

		# Unescape = to - for leading character
		ytids = list(self.args.chapter_edit)
		ytids = ['-' + _[1:] for _ in ytids if _[0] == '='] + [_ for _ in ytids if _[0] != '=']

		for ytid in ytids:
			print(ytid)

			row = self.db.v.select_one('*', '`ytid`=?', [ytid])
			if row is None:
				print("\tNot a recognized video")
				abort = True
				continue

			row = dict(row)

			# Get file name
			fname = self.db.get_v_fname(ytid)
			if not os.path.exists(fname):
				print("\tNot downloaded, use --download to get the video first")
				abort = True
				continue

			if row['chapters'] is None:
				print("\tNo chapter information provided yet")
			else:
				print("\tChapter information found")

			# Save data
			dat[ytid] = dict(row)
			dat[ytid]['path'] = fname

		if abort:
			sys.exit(-1)

		print("-"*80)
		print("Edit chapter information")
		print()

		for ytid in ytids:
			print("%s - %s" % (ytid, dat[ytid]['title']))

			while True:
				z = inputopts("\tChapters: (p)rint, (e)dit, (d)escription dump, add (o)ffset, (C)continue, (q)uit? ")
				if z == 'p':
					if dat[ytid]['chapters'] is None:
						print("\tNo chapter data")
					else:
						chaps = dat[ytid]['chapters']
						maxlen = 0
						for chap in chaps:
							maxlen = max(maxlen, len(chap[0]))

						print()
						for i,chap in enumerate(chaps):
							print("\t\t%d) %*s -- %s" % (i+1,maxlen, chap[0], chap[1]))

				elif z == 'e':
					p = self.db.get_v_fname(ytid, suffix='info.json')

					chaps = ""
					if os.path.exists(p):
						with open(p, 'r') as f:
							txt = f.read()
						o = json.loads(txt)
						if 'chapters' in o and o['chapters'] is not None:
							chaps = "# Chapter information obtained from info.json (yank and paste and strip off leading #):\n"
							for c in o['chapters']:
								s = sec_str(c['start_time'])
								chaps += "# %s\t%s\n" % (s, c['title'])
						else:
							chaps = "# No chapter information found in info.json file (%s)\n" % p
					else:
						chaps = "# No chapter information found in info.json file (file not found)\n"

					with tempfile.NamedTemporaryFile(mode='w+') as f:
						f.write("# %s\n" % ytid)
						f.write("#  Title:      %s\n" % dat[ytid]['title'])
						f.write("#  Duration:   %d sec (%s)\n" % (dat[ytid]['duration'], sec_str(dat[ytid]['duration'])))
						f.write("#  Published:  %s\n" % dat[ytid]['ptime'])
						f.write("#  Accessed:   %s\n" % dat[ytid]['atime'])
						f.write("#  Downloaded: %s\n" % dat[ytid]['utime'])
						f.write("#\n")
						f.write("# Chapter information consists of two columns separated by a tab, first column is a time stamp in HH:MM:SS format and the second column is the chapter name.\n")
						f.write("# Every line with # is discarded.\n")
						f.write("# Empty lines are ignored\n")
						f.write("#\n")

						if len(chaps):
							f.write(chaps)
							f.write("#\n")

						f.write("# HH:MM:SS			Title\n")
						f.write("\n")

						try:
							if dat[ytid]['chapters'] is not None:
								chaps = dat[ytid]['chapters']
								print(['chaps', chaps])
								for chap in chaps:
									print(['chap', chap])
									f.write("%s\t%s\n" % (chap[0],chap[1]))
						except Exception as e:
							traceback.print_exc()
							print("Caught exception, will load blank screen")

						f.seek(0)

						# Edit with vim
						subprocess.run(['vim', f.name])

						# Read file contents
						f.seek(0)
						z = f.read()
						z = z.split('\n')
						z = [_ for _ in z if len(_) != 0]
						z = [_ for _ in z if _[0] != '#']

						y = []
						for line in z:
							parts = line.split('\t',1)
							parts = [_.strip() for _ in parts]
							# Fix an easy typo
							parts[0] = parts[0].replace(';',':')
							y.append(parts)

						self.db.begin()
						if not len(y):
							self.db.v.update({'ytid': ytid}, {'chapters': None})
						else:
							self.db.v.update({'ytid': ytid}, {'chapters': json.dumps(y)})
						self.db.commit()
						dat[ytid]['chapters'] = y

				elif z == 'd':
					fname = dat[ytid]['path']
					iname = os.path.splitext(fname)[0] + '.info.json'

					if os.path.exists(iname):
						with open(iname) as f:
							z = f.read()
						z = json.loads(z)
						print(z['description'])

				elif z == 'C':
					break
				elif z == 'q':
					sys.exit(0)
				elif z == 'o':
					while True:
						print("Adding an offset can be arbitrary (input an integer number of seconds to shift) or")
						print("specify a chapter's new offset to calculate the offset to shift all chapters.")
						z = inputopts("\tOffset: (a)rbitrary, (c)hapter time, (b)ack to main menu")
						if z == 'a':
							if dat[ytid]['chapters'] is None:
								print("\tNo chapter data")
								print("Cannot adjust offset")
								break
							else:
								chaps = dat[ytid]['chapters']
								maxlen = 0
								for chap in chaps:
									maxlen = max(maxlen, len(chap[0]))

								print()
								print("Current chapter information")
								for i,chap in enumerate(chaps):
									print("\t\t%d) %*s -- %s" % (i+1,maxlen, chap[0], chap[1]))

								sec = input("Enter number of seconds: ")
								sec = int(sec)

								print()
								print("Proposed chapter information")
								for i,chap in enumerate(chaps):
									c = sec_str(sec + t_to_sec(chap[0]))
									print("\t\t%d) %*s -- %s" % (i+1,maxlen, c, chap[1]))

								z = inputopts("(A)ccept or (r)eject change")
								if z == 'a':
									# Adjust times
									for i,chap in enumerate(chaps):
										chap[0] = sec_str(sec + t_to_sec(chap[0]))

									self.db.begin()
									self.db.v.update({'ytid': ytid}, {'chapters': json.dumps(chaps)})
									self.db.commit()
									dat[ytid]['chapters'] = chaps

								elif z == 'r':
									continue
								else:
									raise ValueError("Unrecognized input: %s" % z)
							pass
						elif z == 'c':
							if dat[ytid]['chapters'] is None:
								print("\tNo chapter data")
								print("Cannot adjust offset")
								break
							else:
								chaps = dat[ytid]['chapters']
								maxlen = 0
								for chap in chaps:
									maxlen = max(maxlen, len(chap[0]))

								print()
								print("Current chapter information")
								for i,chap in enumerate(chaps):
									print("\t\t%d) %*s -- %s" % (i+1,maxlen, chap[0], chap[1]))

								cnum = input("Enter chapter number to adjust: ")
								cnum = int(cnum)

								print("\t%d) %*s -- %s" % (cnum,maxlen, chaps[cnum-1][0], chaps[cnum-1][1]))
								sec_old = t_to_sec(chaps[cnum-1][0])

								sec = input("Enter new time for this chapter in HH:MM:SS format: ")
								sec = t_to_sec(sec)
								delta = sec - sec_old

								if delta >= 0:
									print("Change in seconds: +%d" % delta)
								else:
									print("Change in seconds: %d" % delta)

								print()
								print("Proposed chapter information")
								for i,chap in enumerate(chaps):
									c = sec_str(delta + t_to_sec(chap[0]))
									print("\t\t%d) %*s -- %s" % (i+1,maxlen, c, chap[1]))

								z = inputopts("(A)ccept or (r)eject change")
								if z == 'a':
									# Adjust times
									for i,chap in enumerate(chaps):
										chaps[i][0] = sec_str(delta + t_to_sec(chap[0]))

									self.db.begin()
									self.db.v.update({'ytid': ytid}, {'chapters': json.dumps(chaps)})
									self.db.commit()
									dat[ytid]['chapters'] = chaps
						elif z == 'b':
							break
						else:
							raise ValueError("Unrecognized input: %s" % z)
				else:
					raise ValueError("Unrecognized input: %s" % z)
				print()

	def chapterize(self):
		pruned = self._prunesleep()

		dname = os.path.dirname(self.db.Filename) + '/CHAPTERIZED'
		if not os.path.exists(dname):
			os.makedirs(dname)

		abort = False

		dat = {}

		print("Checking that videos are present")
		print()

		# Unescape = to - for leading character
		ytids = list(self.args.chapterize)
		ytids = ['-' + _[1:] for _ in ytids if _[0] == '='] + [_ for _ in ytids if _[0] != '=']

		for ytid in ytids:
			print(ytid)

			row = self.db.v.select_one('*', '`ytid`=?', [ytid])
			if row is None:
				print("\tNot a recognized video")
				abort = True
				continue

			row = dict(row)

			# Get file name
			fname = self.db.get_v_fname(ytid)
			if not os.path.exists(fname):
				print("\tNot downloaded, use --download to get the video first")
				abort = True
				continue

			fname_chapsmkv = dname + '/' + fname + '.chapters.mkv'

			if os.path.exists(fname_chapsmkv):
				print("\tAlready chapterized, skipping" % fname_chapsmkv)

			if row['chapters'] is None:
				print("\tNo chapter information provided yet")
				abort = True

			# Save data
			dat[ytid] = dict(row)
			dat[ytid]['path'] = fname

		if abort:
			sys.exit(-1)

		print("-"*80)
		print("Chapterize")
		print()

		for ytid in ytids:
			print("%s -- %s" % (ytid, dat[ytid]['title']))

			fname = dat[ytid]['path']
			fname_chaps = dname + '/%s.chapters.xml' % ytid
			fname_chapsmkv = dname + '/%s.chapters.mkv' % ytid

			print(dat[ytid]['chapters'])

			if not os.path.exists(fname_chaps):
				# Create chapters XML
				cxml = mkvxmlmaker.MKVXML_chapter()
				for v in dat[ytid]['chapters']:
					cxml.AddChapter(v[0], v[1])
				cxml.Save(fname_chaps)

			# Add in chapter info
			args = ['mkvmerge', '-o', fname_chapsmkv, '--chapters', fname_chaps, fname]
			print(" ".join(args))
			subprocess.run(args)

	def split(self):
		pruned = self._prunesleep()

		abort = False

		dat = {}

		print("Checking that video is present")
		print()

		# Get arguments for splitting: YTID OUTPUT_FORMAT FILENAME_FORMAT
		ytid = self.args.split[0]
		fmt = self.args.split[1]
		outfmt = self.args.split[2]

		# Unescape = to - for leading character
		if ytid[0] == '=':
			ytid = '-' + ytid[1:]

		# Check that format is ok
		recognized_formats = ('mkv', 'mp3', 'ogg')
		if ':' in fmt:
			parts = fmt.split(':',1)

			if parts[0] not in recognized_formats:
				print("Format '%s' is not recognized (%s)" % (fmt, recognized_formats))
				sys.exit(-1)

			if parts[0] == 'mp3' and not parts[1].endswith('kbps'):
				print("Format '%s' must end with 'kbps' to indicate bitrate" % fmt)
				sys.exit(-1)

		else:
			if fmt not in recognized_formats:
				print("Format '%s' is not recognized (%s)" % (fmt, recognized_formats))
				sys.exit(-1)


		# Path is %YTD%/SPLIT/YTID/
		dname = os.path.dirname(self.db.Filename) + '/SPLIT'
		if not os.path.exists(dname):
			os.makedirs(dname)

		dname = os.path.dirname(self.db.Filename) + '/SPLIT/' + ytid + '/'
		if not os.path.exists(dname):
			os.makedirs(dname)


		# Get video data
		row = self.db.v.select_one('*', '`ytid`=?', [ytid])
		if row is None:
			print("\tNot a recognized video")
			sys.exit(-1)
		row = dict(row)

		# Get file name
		fname = self.db.get_v_fname(ytid)
		if not os.path.exists(fname):
			print("\tNot downloaded, use --download to get the video first")
			sys.exit(-1)

		# Find a thumbnail
		fname_thumb = fname.replace('.mkv', '_0.jpg')
		if not os.path.exists(fname_thumb):
			fname_thumb = fname.replace('.mkv', '_1.jpg')
			if not os.path.exists(fname_thumb):
				fname_thumb = fname.replace('.mkv', '_2.jpg')
				if not os.path.exists(fname_thumb):
					fname_thmb = None

		# Save data
		dat[ytid] = dict(row)
		dat[ytid]['path'] = fname

		print("-"*80)
		print("Split")
		print()

		print("%s -- %s" % (ytid, dat[ytid]['title']))

		# Source file path
		fname = dat[ytid]['path']

		# Debug
		print(dat[ytid]['chapters'])

		# Need to get start time of subsequent chapter to pass as -to parameter to ffmpeg to stop at the end of the chapter
		for i in range(len(dat[ytid]['chapters'])-1):
			c = dat[ytid]['chapters'][i]
			n = dat[ytid]['chapters'][i+1]

			dat[ytid]['chapters'][i] = (c[0], n[0], c[1])
		# duration paramter for th elast chapter is None so that it reads to the end of the original file
		dat[ytid]['chapters'][-1] = (dat[ytid]['chapters'][-1][0], None, dat[ytid]['chapters'][-1][1])

		# Iterate over chapters and output
		num = 1
		out = {}
		fnames = {}
		run_args = ['ffmpeg', '-y', '-accurate_seek', '-i', fname]
		for t,dur,cname in dat[ytid]['chapters']:
			# Gather possible {fields} for formatting
			z = {'N': "%0*d" % (len(str(len(dat[ytid]['chapters']))),num), 'total': len(dat[ytid]['chapters']), 'ytid': ytid, 'name': cname}
			if self.args.artist:
				z['artist'] = self.args.artist
			if self.args.album:
				z['album'] = self.args.album
			if self.args.year:
				z['year'] = self.args.year
			if self.args.genre:
				z['genre'] = self.args.genre

			# Format file name as specified
			fname_out = outfmt.format(**z)

			# Fix some characters that can't be in names
			fname_out = title_to_name(fname_out)

			#args = self._make_convert_args(fmt, fname, dname + fname_out, start=t, duration=dur)
			#print(" ".join(args))
			#subprocess.run(args)

			# Add extra arguments depending on the output format
			if fmt.startswith('mp3:'):
				fname_out += '.mp3'
				# format must be like "mp3:196kbps" to get the right bitrate passed
				extra_args = ['-c:a', 'libmp3lame', '-b:a', fmt.split(':',1)[-1][0:-3]]
			elif fmt.startswith('ogg:'):
				fname_out += '.ogg'
				# format must be like "ogg:5.0" to get the right quality passed
				# -map 0:a:0 maps the audio but not the video
				extra_args = ['-map', '0:a:0', '-c:a', 'libvorbis', '-q:a', fmt.split(':',1)[-1]]
			else:
				raise Exception("Unrecognized output format '%s'" % fmt)

			# Wait to get suffix
			fnames[num] = fname_out

			if dur is None:
				run_args += extra_args + ['-ss', str(t), dname + fname_out]
			else:
				run_args += extra_args + ['-ss', str(t), '-to', str(dur), dname + fname_out]

			parms = {
				'name': cname,
				'N': num,
				'total': len(dat[ytid]['chapters']),
			}
			if self.args.artist:
				parms['artist'] = self.args.artist
			if self.args.album:
				parms['album'] = self.args.album
			if self.args.year:
				parms['year'] = self.args.year
			if self.args.genre:
				parms['genre'] = self.args.genre

			out[num] = {
				'parms': parms,
				'fname': dname + fname_out,
			}

			num += 1

		# Dice it up
		print(" ".join(run_args))
		subprocess.run(run_args)

		if fname_thumb is not None:
			# Convert retrieved thumbnail to a jpg
			args = ['convert', fname_thumb, dname + 'album.jpg']
			print(" ".join(args))
			subprocess.run(args)

			#TODO: rescale jpg?

			# Merge mp3 with jpg as ID3 2.3 tag
			num = 1
			while num in out:
				fname = fnames[num]

				args = ['ffmpeg', '-i', dname + fname, '-i', dname + 'album.jpg', '-map', '0:0', '-map', '1:0', '-c', 'copy', '-id3v2_version', '3', '-metadata:s:v', 'title=Album cover', '-metadata:s:v', 'comment=Cover (front)', '-y', dname + 'temp' + fname[-4:]]
				print(" ".join(args))
				subprocess.run(args)

				os.rename(dname + 'temp' + fname[-4:], dname + fname)

				num += 1

		# Update metadata
		num = 1
		while num in out:
			parms = out[num]['parms']
			fname = out[num]['fname']

			# If the name format is specified, then pass that
			# Eg, Subaru requires the track number to be in the title as it alpha sorts by title and ignores the track number
			if self.args.format_name:
				self._tag_file(fmt, parms, fname, format_name=self.args.format_name)
			else:
				# Use default name formatting
				self._tag_file(fmt, parms, fname)

			num += 1

	def convert(self):
		pruned = self._prunesleep()

		abort = False

		dat = {}

		print("Checking that video is present")
		print()

		# Get arguments for splitting: YTID OUTPUT_FORMAT FILENAME_FORMAT
		ytid = self.args.split[0]
		fmt = self.args.split[1]
		outfmt = self.args.split[2]

		# Unescape = to - for leading character
		if ytid[0] == '=':
			ytid = '-' + ytid[1:]

		# Check that format is ok
		recognized_formats = ('mp3', 'ogg')
		if ':' in fmt:
			parts = fmt.split(':',1)

			if parts[0] not in recognized_formats:
				print("Format '%s' is not recognized (%s)" % (fmt, recognized_formats))
				sys.exit(-1)

			if parts[0] == 'mp3' and not parts[1].endswith('kbps'):
				print("Format '%s' must end with 'kbps' to indicate bitrate" % fmt)
				sys.exit(-1)

		else:
			if fmt not in recognized_formats:
				print("Format '%s' is not recognized (%s)" % (fmt, recognized_formats))
				sys.exit(-1)


		# Path is %YTD%/CONVERT/YTID/
		dname = os.path.dirname(self.db.Filename) + '/CONVERT'
		if not os.path.exists(dname):
			os.makedirs(dname)

		dname = os.path.dirname(self.db.Filename) + '/CONVERT/' + ytid + '/'
		if not os.path.exists(dname):
			os.makedirs(dname)


		# Get video data
		row = self.db.v.select_one('*', '`ytid`=?', [ytid])
		if row is None:
			print("\tNot a recognized video")
			sys.exit(-1)
		row = dict(row)

		# Get file name
		fname = self.db.get_v_fname(ytid)
		if not os.path.exists(fname):
			print("\tNot downloaded, use --download to get the video first")
			sys.exit(-1)

		# Save data
		dat[ytid] = dict(row)
		dat[ytid]['path'] = fname

		print("-"*80)
		print("Convert")
		print()

		print("%s -- %s" % (ytid, dat[ytid]['title']))

		# Source file path
		fname = dat[ytid]['path']

		# Gather possible {fields} for formatting
		z = {'N': 1, 'ytid': ytid, 'name': cname}
		if self.args.artist:
			z['artist'] = self.args.artist
		if self.args.album:
			z['album'] = self.args.album
		if self.args.year:
			z['year'] = self.args.year
		if self.args.genre:
			z['genre'] = self.args.genre

		# Format file name as specified
		fname_out = outfmt.format(**z)

		# Fix some characters that can't be in names
		fname_out = title_to_name(fname_out)

		args = self._make_convert_args(fmt, fname, dname + fname_out)
		print(" ".join(args))
		subprocess.run(args)

		parms = {
			'name': 'out',
		}
		if self.args.artist:
			parms['artist'] = self.args.artist
		if self.args.album:
			parms['album'] = self.args.album
		if self.args.year:
			parms['year'] = self.args.year
		if self.args.genre:
			parms['genre'] = self.args.genre

		self._tag_file(fmt, parms, dname + fname_out)

	@classmethod
	def _make_convert_args(cls, fmt, fname, fname_out, start=None, duration=None):
		"""
		Take output format type string @fmt (eg, mp3:256kbps, ogg:8.0) and return a list of args
		 suitable to invoke in subproces.run().
		Input file name @fname.
		Output file name @fname_out.
		"""
		# Add extra arguments depending on the output format
		if fmt.startswith('mp3:'):
			fname_out += '.mp3'
			# format must be like "mp3:196kbps" to get the right bitrate passed
			extra_args = ['-c:a', 'libmp3lame', '-b:a', fmt.split(':',1)[-1][0:-3]]
		elif fmt.startswith('ogg:'):
			fname_out += '.ogg'
			# format must be like "ogg:5.0" to get the right quality passed
			# -map 0:a:0 maps the audio but not the video
			extra_args = ['-map', '0:a:0', '-c:a', 'libvorbis', '-q:a', fmt.split(':',1)[-1]]
		else:
			raise Exception("Unrecognized output format '%s'" % fmt)

		# Create ffmpeg arguments
		if start is not None:
			if duration is None:
				return ['ffmpeg', '-accurate_seek', '-i', fname] + extra_args + ['-ss', start, fname_out]
			else:
				return ['ffmpeg', '-accurate_seek', '-i', fname] + extra_args + ['-ss', start, '-to', duration, fname_out]

		else:
			return ['ffmpeg', '-i', fname] + extra_args + [fname_out]


	@classmethod
	def _tag_file(cls, fmt, parms, fname, format_name="{title}"):
		"""
		Tag @fname with data from @parms.
		@fmt provides the means to know what tagging program to use.
		"""

		# Add an ID3 tag if an mp3
		if fmt.startswith('mp3:'):
			if not fname.endswith('.mp3'):
				fname += '.mp3'

			id3tag = []
			if 'artist' in parms:
				id3tag.append('--artist=%s' % parms['artist'])
			if 'album' in parms:
				id3tag.append('--album=%s' % parms['album'])
			if 'year' in parms:
				id3tag.append('--year=%s' % parms['year'])
			if 'genre' in parms:
				id3tag.append('--genre=%s' % parms['genre'])
			if 'N' in parms:
				id3tag.append('--track=%d' % parms['N'])
			if 'total' in parms:
				id3tag.append('--total=%d' % parms['total'])
			if 'name' in parms:
				# Format title as instructed
				v = N_formatter().format(format_name, **parms)
				id3tag.append('--song=%s' % v)

			args = ['id3tag'] + id3tag + [fname]
			print(" ".join(args))
			subprocess.run(args)

		elif fmt.startswith('ogg:'):
			if not fname.endswith('.ogg'):
				fname += '.mp3'

			if 'artist' in parms:
				args = ['vorbiscomment', '-a', '-t', 'ARTIST=' + parms['artist'], fname]
				print(" ".join(args))
				subprocess.run(args)
			if 'album' in parms:
				args = ['vorbiscomment', '-a', '-t', 'ALBUM='+parms['album'], fname]
				print(" ".join(args))
				subprocess.run(args)
			if 'year' in parms:
				args = ['vorbiscomment', '-a', '-t', 'DATE='+parms['year'], fname]
				print(" ".join(args))
				subprocess.run(args)
			if 'genre' in parms:
				args = ['vorbiscomment', '-a', '-t', 'GENRE='+parms['genre'], fname]
				print(" ".join(args))
				subprocess.run(args)
			if 'N' in parms:
				args = ['vorbiscomment', '-a', '-t', 'TRACKNUMBER=%d' % parms['N'], fname]
				print(" ".join(args))
				subprocess.run(args)
			if 'name' in parms:
				# Format title as instructed
				v = N_formatter().format(format_name, **parms)

				args = ['vorbiscomment', '-w', '-t', 'TITLE=%s' % v, fname]
				print(" ".join(args))
				subprocess.run(args)

		elif fmt == 'mkv':
			pass

	def updatenames(self):
		print("Updating file names to v.name or with preferred name")

		where = '`skip`=0'
		if type(self.args.update_names) is list:
			# Filter
			where += " AND (`ytid` in ({0}) or `dname` in ({0}))".format( list_to_quoted_csv(self.args.update_names) )

		res = self.db.v.select(['rowid','ytid','dname','name'], where)

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
			sub_row = self.db.vnames.select_one('name', '`ytid`=?', [ytid])
			if sub_row:
				name = sub_row['name']

			print("\t%d of %d: %s" % (i+1, len(rows), row['ytid']))

			# Find everything with that YTID (glob ignores dot files)
			try:
				renamed = _rename_files(dname, ytid, name)
				if renamed:
					summary['change'].append(ytid)
				else:
					summary['same'].append(ytid)
			except FileNotFoundError:
				print("\t\tNot Exist")
				continue

		print("Same: %d" % len(summary['same']))
		print("Changed: %d" % len(summary['change']))

	def download(self):
		pruned = self._prunesleep()

		filt = []
		if type(self.args.download) is list and len(self.args.download):
			filt = self.args.download
			# I don't know how to get argparse to ignore YTID's that start with a dash, so instead use = sign and substitute now
			filt = ['-' + _[1:] for _ in filt if _[0] == '='] + [_ for _ in filt if _[0] != '=']

		print("Download videos")
		try:
			ret = download_videos(self.db, self.args, filt, ignore_old=self.args.ignore_old)

		except Exception as e:
			ret = sys.exc_info()

		# Send notificaiton via Pushover
		if self.args.notify:
			# Send osmething useful but short
			msg = ",".join(filt)
			if len(msg) > 32:
				msg = msg[:32] + '...'

			if ret == True:
				msg = "Download completed: %s" % msg

			elif ret == False:
				msg = "Download aborted: %s" % msg

			elif type(ret) is tuple:
				traceback.print_exception(*ret)

				errmsg = str(ret[1])
				if len(errmsg) > 32:
					errmsg = errmsg[:32] + '...'

				msg = "Dwonload aborted with exception (%s) for %s" % (errmsg, msg)
			else:
				print([type(ret), ret])
				msg = "Download something: %s" % msg

			pushover.Client().send_message(msg, title="ydl")
			print('notify: %s' % msg)

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
		'skip': [],
	}

	# Sync the lists
	for c_name, rss_ok, rowid in rows:
		__sync_list(args, d, d_sub, ydl_func, c_name, rss_ok, rowid, summary)

	print("\tDone: %d" % len(summary['done']))
	print("\tSkip: %d" % len(summary['skip']))
	print("\tError: %d" % len(summary['error']))
	for ytid in summary['error']:
		print("\t\t%s" % ytid)

	# Update atimes
	d.begin()
	for ytid in summary['done']:
		rowid = mp[ytid]['rowid']

		d_sub.update({'rowid': rowid}, {'atime': _now(), 'title': summary['info'][ytid]['title'], 'uploader': summary['info'][ytid]['uploader']})
	d.commit()

def __sync_list(args, d, d_sub, f_get_list, c_name, rss_ok, rowid, summary):
	"""
	Base function that does all the list syncing.

	This list indicates if RSS is ok to use or not (can be overridden to not use RSS).
	The RSS is used only to indicate that there are new videos to sync.
	If a full sync is needed then __sync_list_full() is called.

	@d is the database object
	@d_sub is table object in @d
	@f_get_list is a function in ydl library that gets videos for the given list (as this is unique for each list type, it must be supplied
	@rss_ok -- can check RSS first
	@summary -- dictionary to store results of syncing each list
	"""

	# Alternate column name (specifically for unnamed channels)
	c_name_alt = None

	# Print the name out to show progress
	if d_sub.Name == 'ch':
		row = d_sub.select_one('alias', "`rowid`=?", [rowid])
		c_name_alt = row[0]
	elif d_sub.Name == 'pl':
		row = d_sub.select_one('skip', "`rowid`=?", [rowid])
		if row['skip']:
			print("\t%s SKIPPED" % c_name)
			summary['skip'].append(c_name)
			return

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

			if url:
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
		return
	else:
		# Fetch full list
		__sync_list_full(args, d, d_sub, f_get_list, summary,   c_name, c_name_alt, new)

def __sync_list_full(args, d, d_sub, f_get_list, summary, c_name, c_name_alt, new):
	"""
	Fetch the full list

	Accessing the channel via youtube-dl is necessary to obtain the order of the channel
	 and to adjust the index values for each video in the list.
	This is, unfortunately, slow at this time as it can take some time to update thousands of entries.

	@args -- argparse result object
	@d -- database object
	@d_sub -- database table object for this particular list
	@f_get_list -- function in ydl library to call to get list of videos
	@summary -- dictionary to store results of syncing each list
	@c_name -- column name that uniquely identifies the list (eg, c.name, ch.name, u.name, pl.ytid)
	@c_name_alt -- alternate column name (namely for unnamed channels)
	@new -- list of new YTID's from RSS feed, None otherwise
	"""

	print("\t\tChecking full list")

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

		d.begin()

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
				title = v.get('title', None)
				name = title_to_name(title)

				# Attempt update then fall back to insert if that fails (eg, rowcount==0)
				r = d.v.update({'ytid': v['ytid']}, {'atime': None, 'title': title, 'name': name})
				if r.rowcount == 0:
					n = _now()
					# FIXME: dname is whatever list adds it first, but should favor
					# the channel. Can happen if a playlist is added first, then the channel
					# the video is on is added later.
					r = d.v.insert(ytid=v['ytid'], ctime=n, atime=None, dname=(c_name_alt or c_name), title=title, name=name, skip=False)

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

def download_videos(d, args, filt, ignore_old):
	# Get total number of videos in the database
	total = d.v.num_rows()

	print("%d videos in database" % total)

	# See how many are skipped
	total = d.v.num_rows("`skip`=1")
	print("\tSkipped: %d" % total)

	skipped = []
	# Check if playlist is skipped
	for ytid in filt:
		row = d.pl.select_one('skip', '`ytid`=?', [ytid])
		if row is not None and row['skip']:
			print("\tPlaylist %s SKIPPED" % ytid)
			skipped.append(ytid)
	# Remove playlist if pl.skip is true
	for ytid in skipped:
		filt.remove(ytid)

	if len(skipped) and not len(filt):
		print("All playlists skipped")
		return


	# Filter
	where = ""
	if type(filt) is list and len(filt):
		# Catch if playlist is provided but v.dname is the channel owner not the playlist (this will catch anything for dname and not just playlists, but should be fine regardless)
		res = d.vids.select('ytid', '`name` in (%s)' % list_to_quoted_csv(filt))
		other_rows = [_['ytid'] for _ in res]

		# Found some, add to the filter list
		# What playlist names were caught and returned rows in the above query will be ignored in the below query (that's the problem of the videos not getting pulled in)
		# so laves those playlist ID's in there
		if len(other_rows):
			filt.extend(other_rows)

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

		ret = _download_video(d, args, ytid, row)
		if ret == False:
			return False
		elif ret is None:
			# Next video
			continue

	# Completed download
	return True

def _download_video(d, args, ytid, row):
	"""Download YTID and handle renaming, if needed"""

	# Get preferred name, if one is set
	alias_row = d.vnames.select_one('name', '`ytid`=?', [ytid])
	alias = None
	if alias_row:
		alias = alias_row['name']

	# Required
	if row['dname'] is None:
		raise ValueError("Expected dname to be set for ytid '%s'" % row['dname'])

	row_sleep = d.v_sleep.select_one(['rowid','t'], 'ytid=?', [ytid])
	if row_sleep is not None:
		# Double check as downloading previous videos may have delayed this video such that
		# the initial pruning may have been just before the sleep time
		now = datetime.datetime.utcnow()
		if row_sleep['t'] > now:
			# Still sleeping
			delta = row_sleep['t'] - now
			print("\t\tVideo sleeping until %s UTC (%s away), skipping for now" % (row_sleep['t'].strftime("%Y-%m-%d %H:%M:%S"), delta))
			return
		else:
			# Remove sleep and carry on
			d.begin()
			d.v_sleep.delete({'rowid': row_sleep['rowid']})
			d.commit()

	# If hasn't been updated, then can do sync_videos(ytid) or just download it with ydl
	# then use the info.json file to update the database (saves a call to youtube)
	if row['atime'] is None:
		print("\t\tVideo not synced yet, will get data from info.json file afterward")

		dat = _download_video_TEMP(d, args, ytid, row, alias)

	# Name was present so just download
	else:
		dat = _download_video_known(d, args, ytid, row, alias)

	if isinstance(dat, dict):
		# Update data
		d.begin()
		d.v.update({"rowid": row['rowid']}, dat)
		d.commit()
	elif dat == False:
		# This on KeyboardInterrupt
		return False
	elif dat is None:
		# Some other non-fatal exception, continue to next video
		return None
	else:
		raise Exception("Unexpected _download_video_* return, aborting: '%s'" % str(dat))


	lang = args.caption_language
	# Empty is all languages
	if not len(lang):
		lang = None
	else:
		lang = lang.split(',')

	# Update row information just in case it got renamed and need accurate name to get info.json
	row = d.v.select_one(['rowid','ytid','title','name','dname','ctime','atime'], 'ytid=?', [ytid])

	_download_captions(d, args, ytid, row, alias, lang)
	_download_update_chapters(d, args, ytid, row, alias)

def _download_video_TEMP(d, args, ytid, row, alias):
	"""Download to TEMP-YTID first, then renamed based on info.json file that gets downloaded"""

	# Keep the real directory name
	dname_real = row['dname']

	# Use name if there happens to be one that is present with atime being null
	if row['name']:
		dname,fname = ydl.db.format_v_names(row['dname'], row['name'], alias, row['ytid'])
	else:
		# If no name is present, use TEMP
		dname,fname = ydl.db.format_v_names(row['dname'], 'TEMP', alias, row['ytid'])

	print("\t\tDirectory: %s" % dname)

	# Make subdir if it doesn't exist (this should have been done with --add)
	if not os.path.exists(dname):
		os.makedirs(dname)

	# Comes in as a list
	rate = None
	if args.rate:
		rate = args.rate[0]


	# Finally do actual download
	ret = _download_actual(d, row['ytid'], fname, dname, rate, not args.noautosleep)
	if ret is None:
		return None
	elif ret == False:
		return False
	elif ret == True:
		# Continue processing
		pass
	else:
		raise NotImplementedError("Unknown return value (%s) in downloading vide %s" % (ret,row['ytid']))


	# Look for info.json file that contains title, uplaoder, etc
	fs = glob.glob("%s/*-%s.info.json" % (dname,ytid))
	if not len(fs):
		raise Exception("Downloaded %s to %s/%s but unable to find info.json file" % (ytid, dname, fname))

	# Load in the meta data
	ret = json.load( open(fs[0], 'r') )

	# Squash non-ASCII characters (I don't like emoji in file names)
	name = ydl.db.title_to_name(ret['title'])

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

	# Single video added and not a part of a channel, move to channel's directory now
	if row['dname'] == "MISCELLANEOUS":
		print("\t\tRenaming out of MISCELLANEOUS directory")
		# This is not ideal (prefer the human friendly channel name but can't get that from
		# info.json file at this time) so use just the channel ID
		dat['dname'] = ret['channel_id']

		try:
			_rename_files(dat['dname'], ytid, name, old_dname=row['dname'])
		except Exception as e:
			print(e)

	# Rename TEMP files
	if not row['name']:
		_rename_files(dname_real, ytid, name)

	return dat

def _download_video_known(d, args, ytid, row, alias):
	"""Download a known video (can down --sync-video before)"""
	if row['name'] is None:
		raise ValueError("Expected name to be set for ytid '%s'" % row['name'])

	# Format name
	dname,fname = ydl.db.format_v_names(row['dname'], row['name'], alias, row['ytid'])
	# Have to escape the percent signs
	fname = fname.replace('%', '%%')

	print("\t\tDirectory: %s" % dname)

	# Make subdir if it doesn't exist
	if not os.path.exists(dname):
		os.makedirs(dname)

	# Comes in as a list
	rate = None
	if args.rate:
		rate = args.rate[0]


	# Finally do actual download
	ret = _download_actual(d, row['ytid'], fname, dname, rate, not args.noautosleep)
	if ret is None:
		return None
	elif ret == False:
		return False
	elif ret == True:
		# Continue processing
		pass
	else:
		raise NotImplementedError("Unknown return value (%s) in downloading vide %s" % (ret,row['ytid']))

	dat = {
		'utime': _now()
	}
	return dat

def _download_actual(d, ytid, fname, dname, rate=None, autosleep=True):
	"""
	Long chain of functions, but this actually downloads the video.
	"""

	# Download mkv file, description, info.json, thumbnails, etc
	try:
		# Escape percent signs
		fname = fname.replace('%', '%%')
		if rate is None:
			ydl.download(ytid, fname, dname)
		else:
			ydl.download(ytid, fname, dname, rate=rate)
	except youtube_dl.utils.DownloadError as e:
		traceback.print_exc()
		txt = str(e)
		if 'Video unavailable' in txt:
			d.begin()
			print("\t\tVideo not available, marking skip")
			d.v.update({"ytid": ytid}, {"skip": True})
			d.commit()
			return None
		elif 'access to members-only content' in txt:
			d.begin()
			print("\t\tVideo not available without paying, marking skip")
			d.v.update({"ytid": ytid}, {"skip": True})
			d.commit()
			return None
		elif 'Sign in to confirm your age' in txt:
			d.begin()
			print("\t\tVideo not available without signing in, marking skip")
			d.v.update({"ytid": ytid}, {"skip": True})
			d.commit()
			return None

		if autosleep:
			if 'begin in a few moments' in txt:
				print("\t\tVideo live shortly (%s)" % txt)
				parts = ['', '1 hour']
			elif 'will begin in ' in txt:
				print("\t\tVideo not available yet (%s)" % txt)
				parts = txt.split('will begin in ')
			elif 'Premieres in ' in txt:
				print("\t\tVideo not available yet (%s)" % txt)
				parts = txt.split('Premieres in ')
			else:
				print("Unrecognized time (%s), arbitrarily pcking one day" % txt)
				parts = ['', '1 day']

			parts = parts[1].split(' ')
			num = parts[0]
			num = int(num)

			t = datetime.datetime.utcnow()
			if 'day' in parts[1]:
				t += datetime.timedelta(days=num)
			elif 'hour' in parts[1]:
				t += datetime.timedelta(hours=num)
			elif 'minute' in parts[1]:
				t += datetime.timedelta(minutes=num)
			elif 'second' in parts[1]:
				t += datetime.timedelta(seconds=num)
			else:
				print("Unrecognized time (%s), arbitrarily picking one day" % txt)
				t += datetime.timedelta(days=1)

			d.begin()
			print("\t\tAuto-sleeping video until: %s" % t.strftime("%Y-%m-%d %H:%M:%S"))
			d.v_sleep.insert(ytid=ytid, t=t)
			d.commit()
			return None

		else:
			return None
	except KeyboardInterrupt:
		# Didn't complete download
		return False
	except:
		# Print it out to see it
		traceback.print_exc()
		# Skip errors, and keep downloading
		return None

	return True

def _download_captions(d, args, ytid, row, alias, lang):
	"""
	Downloads captions
	"""
	# Coerce to a list
	if type(lang) == str:
		lang = [lang]

	print("\t\tLooking for subtitles")
	try:
		# Format name
		path = ydl.db.format_v_fname(row['dname'], row['name'], alias, ytid, 'info.json')

		if not os.path.exists(path):
			print("\t\t\tinfo.json not found")
			# um, ok, just bail
			return

		with open(path, 'r') as f:
			txt = f.read()
		o = json.loads(txt)

		if 'subtitles' in o:
			if lang is None:
				# Get all languages
				lang = o['subtitles'].keys()

			for l in lang:
				if l in o['subtitles']:
					for subo in o['subtitles'][l]:
						url = subo['url']
						ext = subo['ext']

						r = requests.get(url)
						mypath = path.replace('info.json', 'subtitle.' + l + '.' + ext)

						# Don't get if already there, unless being forced
						if os.path.exists(mypath) and not args.force:
							print("\t\t\tFound subtitles for lang '%s' type %s, skipping" % (l,ext))
						else:
							print("\t\t\tWriting subtitles to %s for lang '%s' type %s" % (mypath,l,ext))
							with open(mypath, 'w') as f:
								f.write(r.text)
	except:
		traceback.print_exc()
		return

	print("\t\tLooking for [automatic] captions")
	try:
		# Format name
		path = ydl.db.format_v_fname(row['dname'], row['name'], alias, ytid, 'info.json')

		if not os.path.exists(path):
			print("\t\t\tinfo.json not found")
			# um, ok, just bail
			return

		with open(path, 'r') as f:
			txt = f.read()
		o = json.loads(txt)

		if 'automatic_captions' in o:
			if lang is None:
				# Get all languages
				lang = o['automatic_captions'].keys()

			for l in lang:
				if l in o['automatic_captions']:
					for subo in o['automatic_captions'][l]:
						url = subo['url']
						ext = subo['ext']

						r = requests.get(url)
						mypath = path.replace('info.json', 'caption.' + l + '.' + ext)

						# Don't get if already there, unless being forced
						if os.path.exists(mypath) and not args.force:
							print("\t\t\tFound captions for lang '%s' type %s, skipping" % (l,ext))
						else:
							print("\t\t\tWriting automated captions to %s for lang '%s' type %s" % (mypath,l,ext))
							with open(mypath, 'w') as f:
								f.write(r.text)
	except:
		traceback.print_exc()
		return

def _download_update_chapters(d, args, ytid, row, alias):
	print("\t\tLooking for chapters")
	try:
		row_v = d.v.select_one('chapters', 'ytid=?', [ytid])
		if row_v is not None and row_v['chapters'] is not None:
			print("\t\t\tChapters already found in video, skipping info.json search")
			return

		# Format name
		path = ydl.db.format_v_fname(row['dname'], row['name'], alias, ytid, 'info.json')

		with open(path, 'r') as f:
			txt = f.read()
		o = json.loads(txt)

		chaps = []
		if 'chapters' in o:
			for c in o['chapters']:
				s = c['start_time']
				e = c['end_time']
				t = c['title']

				s_str = sec_str(s)

				if len(chaps) == 0 and s_str != '0:00':
					chaps.append( ('0:00', 'Start') )

				chaps.append( (sec_str(s),t) )

		if len(chaps) != 0:
			print("\t\t\tInserting %d chapters: %s" % (len(chaps), chaps))
			d.begin()
			d.v.update({'ytid': ytid}, {'chapters': json.dumps(chaps)})
			d.commit()
		else:
			print("\t\t\tNo chapter information")

	except:
		traceback.print_exc()
		return

if __name__ == '__main__':
	y = YDL()
	y.main()

