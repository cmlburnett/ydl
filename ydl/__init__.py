# Main work horse
import youtube_dl

# System libraries
import contextlib
import datetime
import glob
import io
import json
import os
import shutil
import subprocess
import sys
import traceback

# My personal library
import mkvxmlmaker
from sqlitehelper import SH, DBTable, DBCol, DBColROWID


from .util import title_to_name, list_to_quoted_csv

class EmptyListError(Exception): pass
class PaymentRequiredException(Exception): pass

# From https://stackoverflow.com/questions/5136611/capture-stdout-from-a-script
@contextlib.contextmanager
def capture():
	oldout,olderr = sys.stdout, sys.stderr
	try:
		out=[io.StringIO(), io.StringIO()]
		sys.stdout,sys.stderr = out
		yield out
	finally:
		sys.stdout,sys.stderr = oldout, olderr
		out[0] = out[0].getvalue()
		out[1] = out[1].getvalue()

def download(ytid, name, dname, write_all_thumbnails=True, add_metadata=True, writeinfojson=True, writedescription=True, writeannotations=True, skip_download=False, skip_if_exists=True, skip_if_fails=True, convert_mp3=False, rate=900000):

	# Options to youtube-dl library to download the video
	opts = {
		'merge_output_format': 'mkv',
		'write_all_thumbnails': write_all_thumbnails,
		'add_metadata': add_metadata,
		'writeinfojson': writeinfojson,
		'writedescription': writedescription,
		'writeannotations': writeannotations,
		'skip_download': skip_download,
		'outtmpl': name,
		'ratelimit': rate,
		'retries': 10,
	}
	with youtube_dl.YoutubeDL(opts) as dl:
		# Attempt download
		cwd = os.getcwd()
		try:
			os.chdir(dname)
			dl.download(['https://www.youtube.com/watch?v=%s'%ytid])
		finally:
			# Always go back to the original working directory
			os.chdir(cwd)


def download_group(*vid, write_all_thumbnails=True, add_metadata=True, writeinfojson=True, writedescription=True, writeannotations=True, skip_download=False, skip_if_exists=True, skip_if_fails=True, convert_mp3=False, rate=900000):
	"""
	Download from youtube using youtube_dl module.
		@vid -- List of unnamed parameters that are considered entries to download
		@write_all_thumbnails -- Writes video thumbnails
		@add_metadata -- Saves metadata
		@writeinfojson -- Writes a JSON to file that includes title, url, formats, thumbnail URLs, description, and more
		@writedescription -- Just the vidoe description in its own file
		@writeannotations -- Saves video annotations as XML
		@skip_download -- Skip downloading the video (downloads metadata though)
		@skip_if_exists -- Skips if the MKV video file exists
		@convert_mp3 -- Invokes ffmpeg to convert the MKV to MP3 format
		@rate -- Maximum rate of download in bytes/sec

	Download entries are 3- or 4-tuples of information.
	- If a 3-tuple, then (YT video id, artist, title)
	- If a 4-tuple, then (YT video id, year, artist, title)

	The output file is written as "ARTIST - TITLE-YTID.mkv" and if convert_mp3 is True then the mp3 file has format
	"ARTIST - TITLE-YTID.mp3". In the 4-tuple format, year is not currently used.
	"""

	# Collect list of failed downloads
	# List of 3-tuples of exception information from sys.exc_info and printed out using traceback.print_exception
	fails = []

	# Collapse into a single list
	vids = [item for sublist in vid for item in sublist]

	idx = 0
	for vid in vids:
		idx += 1
		if len(vid) == 4:
			ytid, year, artist, title = vid
		elif len(vid) == 3:
			ytid, artist, title = vid
			year = None
		else:
			raise TypeError("Video contains wrong info: '%s'" % (str(vid),))

		print("Processing %d of %d: %s by %s at %s" % (idx,len(vids),title,artist,ytid))

		# File formats
		fmp3 = '%s - %s-%s.mp3' % (artist, title, ytid)
		fmkv = '%s - %s-%s.mkv' % (artist, title, ytid)

		if os.path.exists(fmkv) and skip_if_exists:
			# Skip if the mkv file already exists (can truncate file to zero to save space to skip downloading in the future)
			print("\tFound mkv: %s" % fmkv)
		else:
			print("\tDownloading MKV")

			# Options to youtube-dl library to download the video
			opts = {
				'merge_output_format': 'mkv',
				'write_all_thumbnails': write_all_thumbnails,
				'add_metadata': add_metadata,
				'writeinfojson': writeinfojson,
				'writedescription': writedescription,
				'writeannotations': writeannotations,
				'skip_download': skip_download,
				'outtmpl': fmkv,
				'ratelimit': rate,
			}
			with youtube_dl.YoutubeDL(opts) as dl:
				try:
					# Attempt download
					dl.download(['https://www.youtube.com/watch?v=%s'%ytid])

				except youtube_dl.utils.DownloadError:
					if skip_if_fails:
						# If failed, add to list and continue onward
						exc = sys.exc_info()
						print("\tFailed to download")
						fails.append(exc)
					else:
						# If failed, re-raise and stop processing
						raise

		# Convert to mp3 if desired used ffmpeg
		if convert_mp3:
			if os.path.exists(fmp3):
				print("\tFound mp3: %s" % fmkv)
			else:
				print("\tConverting MKV to MP3")
				subprocess.run(['ffmpeg', '-i', fmkv, '-codec:a', 'libmp3lame', '-q:a', '2', fmp3])

	# Print out each fail including the stack and exception information
	if fails:
		print(80*"-")
		print("Failed downloads:")
		print(80*"-")
		for i,f in enumerate(fails):
			print("Failed download: %d of %d" % (i+1,len(fails)))
			traceback.print_exception(*f)
			print(80*"-")

def get_info_video(ytid):
	"""
	Gets video information for video with YouTube id @ytid.
	Returned is a dictionary of ytid, title, duration (in seconds), list of categories, list of tags, list of thumbnails with URLs, and description.
	"""

	opts = {
		'skip_download': True,
		'dumpjson': True,
		'forcejson': True,
		'quiet': True,
	}

	# Have to capture the standard output
	try:
		with capture() as capt:
			with youtube_dl.YoutubeDL(opts) as dl:
				dl.download(['https://www.youtube.com/watch?v=%s' % ytid])
	except youtube_dl.utils.DownloadError as e:
		if "requires payment" in str(e):
			print("\t\tPayment required, skipping")
			raise PaymentRequiredException

	# Get info from the JSON string
	dat = capt[0].split('\n')
	j = json.loads(dat[0])
	ret = {
		'ytid': ytid,
	}

	for k in ['title', 'duration', 'uploader', 'upload_date', 'thumbnails', 'description', 'categories', 'tags']:
		if k in j: ret[k] = j[k]

	return ret

def print_playlist(*vid):
	"""
	Print out the playlist information from get_playlistinfo()
	"""

	info = get_playlistinfo(*vid)
	for plist in info:
		print('Playlist: %s' % plist['plist'])

		for vid in plist['data']:
			print('\t[%d]: "%s" Duration=%d' % (vid['idx'], vid['title'], vid['duration']))

def get_list_user(*vid, getVideoInfo=True):
	"""
	Takes a list of user from a URL of the form http://www.youtube.com/user/NAME and returns the videos on that list.
	Returned is a list of dictionaries of 'idx' of index in the playlist and 'info' that contains get_info_video() information.
	"""

	sub = ['http://www.youtube.com/user/%s/videos' % _ for _ in vid]

	return get_list(sub, getVideoInfo=getVideoInfo)

def get_list_playlist(*vid, getVideoInfo=True):
	"""
	Takes a list of playlist ID's from a URL of the form http://www.youtube.com/playlist?list=PLIST and returns the videos on that list.
	Returned is a list of dictionaries of 'idx' of index in the playlist and 'info' that contains get_info_video() information.
	"""

	sub = ['http://www.youtube.com/playlist?list=%s' % _ for _ in vid]

	return get_list(sub, getVideoInfo=getVideoInfo)

def get_list_channel(*vid, getVideoInfo=True):
	"""
	Takes a list of channel names from a URL of the form http://www.youtube.com/channel/NAME and returns the videos on that named channel.
	Returned is a list of dictionaries of 'idx' of index in the playlist and 'info' that contains get_info_video() information.
	"""

	sub = ['http://www.youtube.com/channel/%s/videos/' % _ for _ in vid]

	return get_list(sub, getVideoInfo=getVideoInfo)

def get_list_c(*vid, getVideoInfo=True):
	"""
	Takes a list of unnamed channel names from a URL of the form http://www.youtube.com/c/NAME and returns the videos on that unnamed channel.
	Returned is a list of dictionaries of 'idx' of index in the playlist and 'info' that contains get_info_video() information.
	"""

	sub = ['http://www.youtube.com/c/%s/videos/' % _ for _ in vid]

	return get_list(sub, getVideoInfo=getVideoInfo)

def get_list(*vid, getVideoInfo=True):
	"""
	Gets a list of videos from the indicated videos from URLs in @vid and returns a list of videos on each list.
	Each list entry contains a dictionary of 'idx' and 'ytid' which are the numerical index in the list and the YouTube ID, respectively.
	If @getVideoInfo is True, then get_info_video() is called on each video and included in the dictionary key 'info'.
	"""

	# Collapse into a single list
	vids = [item for sublist in vid for item in sublist]

	ret = []

	idx = 0
	# Iterate over playlists
	for url in vids:
		idx += 1

		# It was difficult to obtain these options
		#   extract_flat avoids downloading the playlist
		#   dumpjson & forcejson is needed to get just the JSON information
		#   quiet to keep youtube-dl from dumping non-JSON to output
		opts = {
			'extract_flat': True,
			'dumpjson': True,
			'forcejson': True,
			'quiet': True,
		}

		# Playlist info, if found
		pinfo = {
			'title': None,
			'uploader': None,
		}

		# Ok, youtube-dl sometimes is just returning an empty list and I can't figure out what
		# error is happening. The text output says list is successfully downloaded, but then
		# it doesn't dump any videos for the list. Some sort of graceful failing without actually
		# indicating there was an error. I don't know if this is youtube-dl or youtube itself.
		#
		# Hack is to try a few times and wait for something to actually be returned.
		titles = {}
		ytids = []
		for i in range(3):
			# Have to capture the standard output
			with capture() as capt:
				with youtube_dl.YoutubeDL(opts) as dl:
					dl.download([url])

			lines = capt[0]

			# List of JSON objects, one line per video
			lines = lines.split('\n')
			lines = [_ for _ in lines if len(_)] # Trim off empty newlines
			lines = [json.loads(_) for _ in lines]

			# Get YTID and title
			for line in lines:
				ytid = line['id']

				ytids.append(ytid)
				if 'title' in line:
					titles[ytid] = line['title']

			# Get playlist information
			# FIXME: doesn't seem to find the playlist entry
			pi = [_ for _ in lines if _['_type'] == 'playlist']
			if pi:
				if 'title' in pi: pinfo['title'] = pi['title']
				if 'uploader' in pi: pinfo['uploader'] = pi['uploader']

			# If got something, then break
			if len(ytids):
				break

		# Didn't get any videos (see above issue), then throw exception
		if not len(ytids):
			print
			raise EmptyListError("No entries found for list '%s'" % url)

		subret = []

		# Iterate over the playlist items
		subidx = 0
		for ytid in ytids:
			subidx += 1

			# Download video information
			if getVideoInfo:
				# Download video information
				k = get_videoinfo(ytid)

				z = {'idx': subidx, 'ytid': ytid}
				z['info'] = k

				# Appending to list
				subret.append(z)

			# Don't download video information, just do index and YT id
			else:
				if ytid in titles:
					subret.append( {'idx': subidx, 'ytid': ytid, 'title': titles[ytid]} )
				else:
					subret.append( {'idx': subidx, 'ytid': ytid} )


		# Accumulate list information
		ret.append( {'idx': idx, 'url': url, 'info': subret, 'title': pinfo['title'], 'uploader': pinfo['uploader']} )

	return ret

def merge_playlist(*vid, rate=900000):
	"""
	Downloads a playlist and merges them into a concatenated single video file.
	Chapter information provided then permits slicing up the concatened file into chapters (presumable where each original file is a chapter now).

	Each @vid argument is a list of dictionaries where each dictionary consists of:
		plist -- Playlist ID on youtube
		name -- Final merged file name base
		chapters -- list of 2-tuples containing (time of chapter start as HH:MM:SS.ssss string, chapter name)

	A directory is created based on @name, and youtube_dl is used to download the entire playlist into this directory.
	Each playlist item is saved as the index of the playlist (eg, 01.mkv, 02.mkv).
	The files are merged into a single mkv '@NAME@.mkv'.
	The chapter information is written to XML and then merged into the final output '@NAME@.chapters.mkv'.

	The icon file for the concatenated file is assumed to be the icon for the first item in the play list.

	@vid -- List of dictionaries that are the playlists and their info
	@rate -- Maximum rate of download in bytes/sec
	"""

	# Collapse into a single list
	vids = [item for sublist in vid for item in sublist]

	idx = 0
	for merge in vids:
		idx += 1

		# Store current directory as this downloads into a subdir
		curdir = os.getcwd()
		try:
			ytid = merge['plist']
			fname = merge['name']
			cs = merge['chapters']

			print("Processing %d of %d: %s" % (idx, len(vids), ytid))

			fname_mkv = fname + '-' + ytid + '.mkv'
			fname_jpg = fname + '-' + ytid + '.jpg'
			fname_chaps = fname + '-' + ytid + '.chapters.xml'
			fname_chapsmkv = fname + '-' + ytid + '.chapters.mkv'

			# Merge entire playlist into a single mkv file
			if os.path.exists(fname_mkv):
				print("\tFound download directory, skipping")
			else:
				os.mkdir('./' + fname)
				os.chdir('./' + fname)

				opts = {
					'write_all_thumbnails': True,
					'merge_output_format': 'mkv',
					'outtmpl': '%(playlist_index)s',
					'ratelimit': 900000,
				}
				with youtube_dl.YoutubeDL(opts) as dl:
					dl.download(['http://www.youtube.com/playlist?list=%s' % ytid])

				tomerge = glob.glob('*.mkv')
				tomerge = sorted(tomerge)
				tomerge = ["file '%s'"%_ for _ in tomerge]
				tomerge = '\n'.join(tomerge)

				with open('tomerge.txt', 'w') as f:
					f.write(tomerge)

				subprocess.run(['ffmpeg', '-f', 'concat', '-i', 'tomerge.txt', '-c', 'copy', '../' + fname_mkv])

				# Go back to root directory
				os.chdir(curdir)

			# Assume first playlist item for the icon
			if os.path.exists(fname_jpg):
				print("\tFound JPG file, skipping")
			else:
				os.chdir('./' + fname)

				# Get all jpegs, and take the first one
				jpgs = glob.glob('*.jpg')
				jpgs = sorted(jpgs)
				shutil.copyfile(jpgs[0], '../' + fname_jpg)

				# Go back to root directory
				os.chdir(curdir)

			# Make chapters XML file if not present
			if os.path.exists(fname_chaps):
				print("\tFound chapters XML file, skipping")
			else:
				cxml = mkvxmlmaker.MKVXML_chapter()
				for c in cs:
					cxml.AddChapter(*c)
				cxml.Save(fname_chaps)

			# Merge in chapters file into the mkv file
			if os.path.exists(fname_chapsmkv):
				print("\tFound final output file '%s', skipping" % fname_chapsmkv)
			else:
				subprocess.run(['mkvmerge', '-o', fname_chapsmkv, '--chapters', fname_chaps, fname_mkv])

		finally:
			os.chdir(curdir)

def _findentry(ytid, vids):
	"""
	Helper function to find entry with YT id @ytid in list of vids (same list used to provide to download().
	"""
	for row in vids:
		if row[0] == ytid:
			return row[2] + ' - ' + row[3] + '-' + row[0]

	return None

def add_chapters(chapters, vids):
	"""
	Merge downloaded MVK files with chapter information as described in @chapters above.
	This must go second in order to, you know, have something to merge.
	Utilizes my custom mkvxmlmaker library and the mkvmerge CLI tool to do the work.

	@chapters -- list of dictionaries where each dictionary includes 'ytid' for the YouTube ID and 'chapters' which is a list of 2-tuples containing (time of chapter start as HH:MM:SS.ssss string, chapter name).
	@vids -- list of videos as passed to download() that is a list of tuples where first tuple item is the YouTube ID (permits extracting file name from this information without repeating it).
	"""

	idx = 0
	for chaps in chapters:
		idx += 1

		ytid = chaps['ytid']
		cs = chaps['chapters']

		print("Processing %d of %d: %s" % (idx, len(chapters), ytid))

		# Find entry as a file name
		fname = _findentry(ytid, vids)
		if fname is None:
			raise ValueError("Cannot find video with id '%s'" % ytid)

		# Make various file names
		fname_mkv = fname + '.mkv'
		fname_chaps = fname + '.chapters.xml'
		fname_chapsmkv = fname + '.chapters.mkv'

		# Already exists, skip it
		if os.path.exists(fname_chapsmkv):
			print("\tFound mkv: %s" % fname_chapsmkv)
			continue

		# Create XML file
		cxml = mkvxmlmaker.MKVXML_chapter()
		for c in cs:
			cxml.AddChapter(*c)

		# Save chapters to XML
		cxml.Save(fname_chaps)

		# Merge files
		subprocess.run(['mkvmerge', '-o', fname_chapsmkv, '--chapters', fname_chaps, fname_mkv])

def download_one_with_chapters(info, chapters, convert_mp3=False):
	"""
	Download a single video and add chapters to it.
	@info is a tuple as supplied to download()
	@chapters is a list of chapter information as supplied to add_chapters()

	Convenience function for calling these two videos.
	"""

	download_group([info], convert_mp3=convert_mp3)
	add_chapters([{'ytid': info[0], 'chapters': chapters}], [info])



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
			DBCol('thumbnails', 'json'),

			# Chapter information, if used, otherwise null
			DBCol('chapters', 'json'),

			# Free text added to describe how to fix downloading this video, manually added
			DBCol('fixcomments', 'text'),
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
			DBCol('atime', 'datetime'),
			DBCol('skip', 'bool')
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

		DBTable("v_sleep",
			DBCol('ytid', 'text'),
			DBCol('t', 'datetime'), # UTC time to consider it "skipped", once passed, this row should be deleted and can be treated as a normal video
		),

	]
	def open(self, rowfactory=None):
		ex = os.path.exists(self.Filename)

		super().open()

		if not ex:
			self.MakeDatabaseSchema()

	def reopen(self):
		super().reopen()

	def get_video(self, ytid):
		return self.v.select_one("*", "`ytid`=?", [ytid])

	def get_user(self, name):
		return self.u.select_one("*", "`name`=?", [name])

	def get_playlist(self, ytid):
		return self.pl.select_one("*", "`ytid`=?", [ytid])

	def get_channel_named(self, name):
		return self.c.select_one("*", "`name`=?", [name])

	def get_channel_unnamed(self, name):
		return self.ch.select_one("*", "`name`=?", [name])


	def add_video(self, ytid, dname):
		return self.v.insert(ytid=ytid, skip=0, dname="MISCELLANEOUS", ctime=_now())

	def add_user(self, name):
		return self.u.insert(name=name, ctime=_now())

	def add_playlist(self, ytid):
		return self.pl.insert(ytid=ytid, ctime=_now())

	def add_channel_named(self, name):
		return self.c.insert(name=name, ctime=_now())

	def add_channel_unnamed(self, name):
		return self.ch.insert(name=name, ctime=_now())

	def get_v(self, filt, ignore_old):
		skipped = []
		# Check if playlist is skipped
		for ytid in filt:
			row = self.pl.select_one('skip', '`ytid`=?', [ytid])
			if row is not None and row['skip']:
				print("\tPlaylist %s SKIPPED" % ytid)
				skipped.append(ytid)
		# Remove playlist if pl.skip is true
		for ytid in skipped:
			filt.remove(ytid)

		if len(skipped) and not len(filt):
			# Everything was skipped
			return []

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
		if name is None:
			name = 'TEMP'

		if alias is None:
			fname = name
		else:
			fname = alias

		if suffix is None:
			return ("%s/%s/%s" % (os.getcwd(), dname, ytid[0]), "%s-%s" % (fname, ytid))
		else:
			return ("%s/%s/%s" % (os.getcwd(), dname, ytid[0]), "%s-%s.%s" % (fname, ytid, suffix))

	@classmethod
	def format_v_fname(cls, dname, name, alias, ytid, suffix=None):
		return "/".join( cls.format_v_names(dname, name, alias, ytid, suffix) )

	@staticmethod
	def title_to_name(t):
		return title_to_name(t)

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

def _now():
	""" Now """
	return datetime.datetime.utcnow()

