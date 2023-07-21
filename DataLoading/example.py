


def generate_keys(lat,lon):
    # Handle rounding differently for positive and negative values
    p1 = round(lat * 100)
    c1 = p1 - 1 if p1 < 0 else p1
    k1 = c1 - (c1 % 5)  # calculate the closest multiple of 5

    p2 = round(lon * 100)
    c2 = p2 - 1 if p2 < 0 else p2
    k2 = c2 - (c2 % 5)  # calculate the closest multiple of 5

    print(f'p1 : {p1},p2 : {p2}, c1 : {c1}, c2 : {c2}, k1 : {k1}, k2 : {k2}')
    
    return k1, k2

if __name__ == "__main__":
    k1, k2 = generate_keys(30.125, 0.04168128967285156)