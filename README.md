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
./laq -i AVROFILE -c lua_inline -p "SCRIPT_BODY"
```

## lua script

```bash
./laq -i AVROFILE -c lua_script -p PATH_TO_SCRIPT
```

## dump

```bash
./laq -i AVROFILE -c cat
```

## field printer (works only on OSX)

```bash
./laq -i AVROFILE -c field_print -p "path.to.field"
```
