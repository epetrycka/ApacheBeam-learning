with open('./data/defaulter_customers-00000-of-00001') as file:
    defaulters = list()
    for line in file.readlines():
        record = line.split(', ')
        defaulters.append(record)
    
    sorted_Def = sorted(defaulters, key=lambda x: x[1])
    for line in sorted_Def:
        print(line)