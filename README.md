laq
===

# compile:

```bash
./build.sh
cmake .
make
```

# run:

```bash
./laq --help
```

# examples
## inline lua

```bash
# print only fiel0.field1 of first record from *.avro
./laq -i *.avro -c lua_inline -p "print(r.field0.field1)" -n 1
```

## lua script

```bash
# print only field0.field1 of each record from *.avro
# script.lua: return function(r) print(r.field0.field1) end
./laq -i *.avro -c lua_script -p script.lua
```

## dump

```bash
# dump whole file
./laq -i *.var -c cat
```

## field printer (works only on OSX)

```bash
# print only field0.field1 of each record from *.avro
./laq -i *.avro -c field_print -p "field0.field1"
```
