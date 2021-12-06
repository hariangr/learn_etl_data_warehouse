
def scientific_notation_9_kelurahan_id_to_int(cell):
    # Kelurahan Id ditulis dengan 9 digit seperti 6171030001, dan kadang dianggap menjadi notasi ilmiah seperti 6.171031001E9
    # Ubah notasi ilmiah menjadi string of int, lalu ubah menjadi int
    a = cell.replace(".", "").replace("E9", "")
    return int(a)


def remove_after_dot(s):
    # Dimensi Merchant
    return s.split('.')[0]


def split_date_process_start(row):
    # Transform date_process menjadi date_start dan date_end dari str
    s = row['date_process']
    splitted = s.split(" s/d ")
    return splitted[0]


def split_date_process_end(row):
    # Transform date_process menjadi date_start dan date_end dari str
    s = row['date_process']
    splitted = s.split(" s/d ")
    return splitted[1]


def get_lat(cell):
    # Atasi "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
    clean = cell.replace("\t", "").replace(" ", "")
    split_by_comma = clean.split(",")
    if len(split_by_comma) == 2:
        return float(split_by_comma[0].strip())
    elif len(split_by_comma) == 1:
        return float(clean[: clean.find('-', 1)])
    else:
        raise Exception("Format error again")


def get_lng(cell):
    # Complicated karena transaction_from_latlng ada data yang rusak
    # Atasi "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
    clean = cell.replace(r"\t", "").replace(" ", "")
    # Atasi >>-0.03844709999999999,109.3272303 																															-0.03844709999999999<<
    clean = cell.split(
        r" \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t")[0]
    clean = clean.split(' ')[0]
    split_by_comma = clean.split(",")
    return float(split_by_comma[1].strip())
    # try:
    #     if len(split_by_comma) == 2:
    #         return float(split_by_comma[1].strip())
    #     elif len(split_by_comma) == 1:
    #         return float(clean[clean.find('-', 1) : ])
    #     else:
    #         raise Exception("Format error again")
    # except:
    #     print(f">>{repr(cell)}<<")
