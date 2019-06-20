import os.path
import subprocess
import sys
import traceback
import youtube_dl

def download(*vid, write_all_thumbnails=True, add_metadata=True, writeinfojson=True, writedescription=True, writeannotations=True, skip_download=False, skip_if_exists=True, skip_if_fails=True, convert_mp3=False):
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
				'ratelimit': 900000,
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


