#Ben Dayn 10/10/1994 9043318104 
import names
import random 
import argparse

# Args as follows : filen

parser = argparse.ArgumentParser(description=' College students data generator ')
parser.add_argument('-o', action="store", dest="outputfile", help='provide output file name', default='college_students.txt')
parser.add_argument('-n', action="store", dest="num",  type=int , help="nubmer of rows in the dataset" , default=40000)
results = parser.parse_args()

f = open(results.outputfile, "w")
for a in range(1,results.num):
	name  = names.get_full_name()
	month = str(random.randint(1, 12))
	day = str(random.randint(1, 30))
	year = str(random.randint(1988, 2000))
	phone_number = str(random.randint(2000000000, 9999999999))
	line = name + " " + month+"/" + day+ "/" + year + " " + phone_number + "\n"
	f.write(line)
f.close()



