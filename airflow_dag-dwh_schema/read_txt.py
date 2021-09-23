filename = "../data_store/I80_davis.txt"
lines = open(filename).readlines(1)
# print(len(lines))
for line in lines:
    print(line.strip('\n').split(","))
    line = line.lstrip("\n").strip("")
    print(len(line.split(",")))