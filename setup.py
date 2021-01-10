from distutils.core import setup

majv = 1
minv = 4

setup(
	name = 'ydl',
	version = "%d.%d" %(majv,minv),
	description = "Python module that wraps youtube-dl",
	author = "Colin ML Burnett",
	author_email = "cmlburnett@gmail.com",
	url = "https://github.com/cmlburnett/ydl",
	packages = ['ydl'],
	package_data = {'ydl': ['ydl/__init__.py', 'ydl/__main__.py', 'ydl/util.py', 'ydl/fuse.py']},
	classifiers = [
		'Programming Language :: Python :: 3.7'
	]
)
