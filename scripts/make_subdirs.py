
import glob
import os
import sys

import ydl

test = False

def main():
	db = ydl.db('ydl.db')
	db.open()

	pl = {}
	c = {}
	ch = {}
	u = {}
	v = {}
	notfound = []
	for a in sys.argv[1:]:

		res = db.pl.select_one(['rowid'], '`ytid`=?', [a])
		if res is not None:
			pl[a] = res['rowid']
			continue

		res = db.c.select_one(['rowid'], '`name`=?', [a])
		if res is not None:
			c[a] = res['rowid']
			continue

		res = db.u.select_one(['rowid'], '`name`=?', [a])
		if res is not None:
			u[a] = res['rowid']
			continue

		res = db.ch.select_one(['rowid'], '`name`=? or `alias`=?', [a,a])
		if res is not None:
			ch[a] = res['rowid']
			continue

		res = db.v.select_one(['rowid'], '`ytid`=?', [a])
		if res is not None:
			v[a] = res['rowid']
			continue

		notfound.append(a)

	if len(notfound):
		print("The following not found, aborting:")
		for a in notfound:
			print(a)
		sys.exit()


	# ------------ PLAYLISTS --------------
	for ytid,rowid in pl.items():
		row = db.pl.select_one('*', '`rowid`=?', [rowid])
		row = dict(row)

		res = db.vids.select('ytid', '`name`=?', [ytid])
		rows = [dict(_) for _ in res]

		print(row['ytid'])
		for row in rows:
			files = glob.glob('%s/*%s*' % (ytid, row['ytid']))
			for f in files:
				p = os.path.split(f)
				p = list(p)
				if len(p) == 2:
					p.insert(1, row['ytid'][0])
					fnew = '/'.join(p)
					d = ytid + '/' + row['ytid'][0]
					if not os.path.exists(d):
						os.mkdir(d)
					print([d, f,fnew])
					if not test:
						os.rename(f, fnew)


	# ------------ channels --------------
	for ytid,rowid in ch.items():
		row = db.ch.select_one('*', '`rowid`=?', [rowid])
		row = dict(row)

		res = db.vids.select('ytid', '`name`=?', [ytid])
		rows = [dict(_) for _ in res]

		print(row['name'])
		for row in rows:
			files = glob.glob('%s/*%s*' % (ytid, row['ytid']))
			for f in files:
				p = os.path.split(f)
				p = list(p)
				if len(p) == 2:
					p.insert(1, row['ytid'][0])
					fnew = '/'.join(p)
					d = ytid + '/' + row['ytid'][0]
					if not os.path.exists(d):
						os.mkdir(d)
					print([d, f,fnew])
					if not test:
						os.rename(f, fnew)

	for ytid,rowid in c.items():
		row = db.c.select_one('*', '`rowid`=?', [rowid])
		row = dict(row)

		res = db.vids.select('ytid', '`name`=?', [ytid])
		rows = [dict(_) for _ in res]

		print(row['name'])
		for row in rows:
			files = glob.glob('%s/*%s*' % (ytid, row['ytid']))
			for f in files:
				p = os.path.split(f)
				p = list(p)
				if len(p) == 2:
					p.insert(1, row['ytid'][0])
					fnew = '/'.join(p)
					d = ytid + '/' + row['ytid'][0]
					if not os.path.exists(d):
						os.mkdir(d)
					print([d, f,fnew])
					if not test:
						os.rename(f, fnew)

	# ------------ users --------------
	for ytid,rowid in u.items():
		row = db.u.select_one('*', '`rowid`=?', [rowid])
		row = dict(row)

		res = db.vids.select('ytid', '`name`=?', [ytid])
		rows = [dict(_) for _ in res]

		print(row['name'])
		for row in rows:
			files = glob.glob('%s/*%s*' % (ytid, row['ytid']))
			for f in files:
				p = os.path.split(f)
				p = list(p)
				if len(p) == 2:
					p.insert(1, row['ytid'][0])
					fnew = '/'.join(p)
					d = ytid + '/' + row['ytid'][0]
					if not os.path.exists(d):
						os.mkdir(d)
					print([d, f,fnew])
					if not test:
						os.rename(f, fnew)

	# ------------ videos --------------
	for ytid,rowid in v.items():
		row = db.v.select_one('*', '`rowid`=?', [rowid])
		row = dict(row)

		print(row['ytid'])
		for row in rows:
			files = glob.glob('%s/*%s*' % (ytid, row['ytid']))
			for f in files:
				p = os.path.split(f)
				p = list(p)
				if len(p) == 2:
					p.insert(1, row['ytid'][0])
					fnew = '/'.join(p)
					d = ytid + '/' + row['ytid'][0]
					if not os.path.exists(d):
						os.mkdir(d)
					print([d, f,fnew])
					if not test:
						os.rename(f, fnew)



if __name__ == '__main__':
	main()
