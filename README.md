# py-simple-db-utils
Some python abstractions to make simple database operations easier.

SimpleDictWriter Example Usage:
```python
    writer = SimpleDictWriter(db)
    writer.write_dict("myTable", {'id':1234,'value':'mydata'}, commit=True)
```

SimpleDictReader Example Usage:
```python
    reader = SimpleDictReader(db)
    reader.read_dict("table_name")
```

Disclaimer:
This is currently a work in progress, and it only supports MySQL.  Postgres support is possible with minor changes.
