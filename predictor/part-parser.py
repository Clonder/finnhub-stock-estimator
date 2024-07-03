from pathlib import Path


def main():
    parts = searching_all_files(r'../data/')

    output = open('chunk2.json', 'w')

    for part in parts:
        f2 = open(part, 'r')
        # appending the contents of the second file to the first file
        output.write(f2.read())

    output.close()

def searching_all_files(path):
    p = Path(path).glob('**/*.json')
    files = [x for x in p if x.is_file()]
    return files




if __name__ == '__main__':
    main()
