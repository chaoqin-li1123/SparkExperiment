def diskstats_parse(dev=None):
    file_path = '/proc/diskstats'
    result = {}
    columns_disk = ['m', 'mm', 'dev', 'reads', 'rd_mrg', 'rd_sectors',
                    'ms_reading', 'writes', 'wr_mrg', 'wr_sectors',
                    'ms_writing', 'cur_ios', 'ms_doing_io', 'ms_weighted']

    columns_partition = ['m', 'mm', 'dev', 'reads', 'rd_sectors', 'writes', 'wr_sectors']

    lines = open(file_path, 'r').readlines()
    reads = 0
    writes = 0
    for line in lines:
        if line == '': continue
        metrics = line.split()
        reads += int(metrics[3])
        writes += int(metrics[7])
    print(reads)
    print(writes)

'''
    for line in lines:
        print(line)
        if line == '': continue
        split = line.split()
        if len(split) == len(columns_disk):
            columns = columns_disk
        elif len(split) == len(columns_partition):
            columns = columns_partition
        else:
            # No match
            continue

        data = dict(zip(columns, split))
        if dev != None and dev != data['dev']:
            continue
        for key in data:
            if key != 'dev':
                data[key] = int(data[key])
        result[data['dev']] = data

    return result


'''


diskstats_parse()
#print(result)