addIndex() - updates index after DB ops

- data, a Schema instance
- cb, callback

****

removeIndex() - updates index after DB removal op

- data, a Schema instance or object with [id] and [name] keys
- cb, callback

****

write() - interface for database writes

- data, a Schema object instance
- cb, callback in node format, err or written instance of Schema

****

read()

- name, name of schema to lookup
- selector, selector object
- cb, callback

****

remove() - interface for deleting records

- data, object: Schema instance representing a record, or an array of record ids, or a record id
                must have a [name] key and either an [id] or [ids] key
- cb, callback

****

find()

- name, name of the collection to search for

returns finder function to add to Schema as static method

****

insert()

- name, string name of the collection to insert into

returns insert function handler to add to Schema as static method

****

remove()

- name, name of the collection to remove items from

returns remove function to add to Schema as static method