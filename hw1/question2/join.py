import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # TODO: implement this class
    key = record[1]
    type_ = record[0]
    value = record[2:]
    mr.emit_intermediate(key, (type_, value))

def reducer(key, list_of_values):
    print len(list_of_values)
    orders = []
    line_items = []
    for tuple_ in list_of_values:
        if tuple_[0] == 'order':
            orders += tuple_[1]
        elif tuple_[0] == 'line_item':
            line_items += tuple_[1]
            
    out = ['order', key]
    out += orders
    out += ['line_item', key]
    out += line_items
    
    mr.emit(out)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
