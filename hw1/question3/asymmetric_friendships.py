import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # TODO: implement this class
    name_one = record[0]
    name_two = record[1]
    mr.emit_intermediate((name_one, name_two), 1)
    mr.emit_intermediate((name_two, name_one), 1)

def reducer(key, list_of_values):
    # TODO: implement this class
    #print key, list_of_values
    if sum(list_of_values) == 1:
        mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
