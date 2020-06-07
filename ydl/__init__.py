# Main work horse
import youtube_dl

# System libraries
import glob
import os
import shutil
import subprocess
import sys
import traceback

# My personal library
import mkvxmlmaker

def download(*vid, write_all_thumbnails=True, add_metadata=True, writeinfojson=True, writedescription=True, writeannotations=True, skip_download=False, skip_if_exists=True, skip_if_fails=True, convert_mp3=False, rate=900000):
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

