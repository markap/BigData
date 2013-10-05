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
    order = None
    line_items = []
    
    for tuple_ in list_of_values:
        if tuple_[0] == 'order':
            order = tuple_[1]
        elif tuple_[0] == 'line_item':
            line_items.append(tuple_[1])
            
    for line_item in line_items:
        mr.emit(['order', key] + order + ['line_item', key] + line_item)
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
