import MapReduce
import sys

mr = MapReduce.MapReduce()



# =============================
# Do not modify above this line

DIMENSION = 5


def mapper(record):
    # TODO: implement this class
    matrix_name = record[0]
    i, j = record[1], record[2]
    value = record[3]
    
    for k in range(0, DIMENSION):
        if matrix_name == "a":
            mr.emit_intermediate((i, k), (value, j))
        elif matrix_name == "b":
            mr.emit_intermediate((k, j), (value, i))
    
    

def reducer(key, list_of_values):
    # TODO: implement this class
    d = {}
    for i in range(0, DIMENSION):
        d[i] = []
    
    for tuple_ in list_of_values:
        value, position = tuple_[0], tuple_[1]
        d[position].append(value)
    
        
    result = 0
    for i in range(0, DIMENSION):
        result += value_or_zero(d[i], 0) * value_or_zero(d[i], 1)
        
    mr.emit((key[0], key[1], result))

def value_or_zero(p, key):    
    return p[key] if len(p) > key else 0      

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
