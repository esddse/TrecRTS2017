import sys
import os
import datetime

# ----------------- config ----------------------
# -----------------------------------------------

runTag     = "PKUICSTRunB3"
rank_dir   = "./RUN/rank/" + runTag + '/'
submit_dir = "./RUN/submit/"
filename   = runTag

with open(submit_dir+filename, "w") as f: pass

# -----------------------------------------------

def write_file(filepath, day, topic_id):
	with open(filepath, "r") as fin:
		with open(submit_dir+filename, "a") as fout:
			for i, line in enumerate(fin):
				info = line.strip().split('\t')
				time = datetime.datetime.strptime(info[2], "%a %b %d %H:%M:%S +0000 %Y")
				format_time = time.strftime('%Y%m%d')
				runTag = filename
				string = '%s\t%s\tQ0\t%s\t%d\t%s\t%s\n' % (format_time,topic_id,info[3],i+1,info[6],runTag)
				fout.write(string)

def main():
	days = [int(day) for day in os.listdir(rank_dir)]
	days.sort()
	for day in days:
		topic_ids = [topic_id for topic_id in os.listdir(rank_dir + '/' + str(day))]
		for topic_id in topic_ids:
			print day, " ", topic_id
			filepath = rank_dir + str(day) + "/" + topic_id
			write_file(filepath, day, topic_id)




if __name__ == "__main__":
	main()