

"""
This output is
"""
#foo = b'\x00\x00\x00\x00(\xb7\xe05\xfcV>\xcf\t\x95j\r^}\xf7\x03Ur3\xb1\xb1?\xe77\xcf\xc6\xf5K'
#print(foo.hex())

dataList = [{'a': 1}, {'b': 3}, {'c': 5}]
for index in range(len(dataList)):
    for key in dataList[index]:
        print(dataList[index][key])


