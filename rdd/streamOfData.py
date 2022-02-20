import time


def parseLine(line):
    # print(line)
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)
    # yield (age, numFriends)


# with open('fakefriends.csv', 'r') as f:
#     for line in f:
#         print(parseLine(line))
#         time.sleep(0.1)

with open('fakefriends.csv', 'r') as f:
    while line := f.readline():
        print(parseLine(line))
        time.sleep(0.1)