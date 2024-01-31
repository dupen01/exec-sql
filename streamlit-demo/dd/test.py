import os

def gc(filepath):
    files = os.listdir(filepath)
    for file in files:
        file_dir = os.path.join(filepath, file)
        if os.path.isdir(file_dir):
            gc(file_dir)
        else:
            print(os.path.join(filepath, file_dir))

# gc('/Users/pengdu/IdeaProjects/personal-project/exec-sql')


ts  = "hello {a}, hi {a}"

a = "world"

ts.format(a=a)

print(ts.format(a=a))
