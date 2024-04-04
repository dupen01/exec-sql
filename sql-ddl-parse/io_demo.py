


a = open('test-ddl.sql', 'r').readlines()
# for x in a:
    # print("-->", x)


b = open('test-ddl.sql', 'r').readline(10000)
print(b)