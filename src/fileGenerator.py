from random import randint
f = open("/Users/Michael/Desktop/patientData.txt", "w")
alphabetList = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
genderList = ["male", "female"]
gradeList = ["0", "1", "2", "3"]
majorList = ["English", "Mathematics", "ComputerScience", "ElectricalEngineering", "Business", "Economics", "Biology", "Law", "PoliticalScience", "IndustrialEngineering"]
i = 0
while i != 4000000:
	i = i + 1
	firstNameLength = randint(1, 10)
	lastNameLength = randint(1, 15)
	firstName = ''
	lastName = ''
	for num in range(0, firstNameLength):
		firstName = firstName + alphabetList[randint(0, 25)]
	for num in range(0, lastNameLength):
		lastName = lastName + alphabetList[randint(0, 25)]
	gender = genderList[randint(0, 1)]
	grade = gradeList[randint(0, 3)]
	major = majorList[randint(0, 9)]
	fault1 = randint(0, 10000000)
	fault2 = randint(0, 1000000)
	if grade == "0":
		if fault1 <= 1000: #fault version 1000/10000000
			age = randint(18000, 19000)
		else: #correct version
			age = randint(18, 19)
	elif grade == "1":
		age = randint(20, 21)
	elif grade == "2":
		age = randint(22, 23)
	else: 
		age = randint(24, 25)
	if major == "ComputerScience":
		if fault2 <= 5000:#fault version 5000/1000000
			age = NaN
	fault3 = randint(0, 5000000)
	if fault3 <= 5000: #fault version 5000/5000000
		f.write(firstName + " " + lastName + " " + gender + " " + grade + " " + age + " " + major + "\n")
	else:
		f.write(firstName + " " + lastName + " " + gender + " " + str(age) + " " + grade + " " + major + "\n")
f.close()