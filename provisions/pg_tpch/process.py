import sys

def main(filename):
    output = ""
    with open(filename, "r") as f:
        in_select = False
        for line in f:
            line = line.strip()
            if in_select:
                output += (" " + line)
                if ";" in line:
                    in_select = False
            else:
                if "select" in line:
                    output += ("\n\n" + line)
                    in_select = True
    with open("out2.sql", "w") as fout:
        fout.write(output)


if __name__=="__main__":
    main("queries.sql")
