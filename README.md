# Datastore Connectivity for DynamoDB(dyndb)


[![Datastore Connectivity library for DynamoDB in Go.](https://goreportcard.com/badge/github.com/adrianwit/dyndb)](https://goreportcard.com/report/github.com/adrianwit/dyndb)
[![GoDoc](https://godoc.org/github.com/adrianwit/dyndb?status.svg)](https://godoc.org/github.com/adrianwit/dyndb)

This library is compatible with Go 1.10+


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)





## Usage:


The following is a very simple example of CRUD operations

```go
package main

import (
	"github.com/viant/dsc"
    _ "github.com/adrianwit/dyndb"
)


type User struct {
	Id int	`autoincrement:"true"`
	Name string
}


func main() {

	config, err := dsc.NewConfigWithParameters("dyndb", "", "", map[string]interface{}{
    		"host":   "127.0.0.1",
    		"dbname": "mydb",
    		"keyColumnName":"id",
    })
	if err != nil {
		log.Fatal(err)
    }
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
    if err != nil {
    	log.Fatal(err)
    }
    }
    
    var users []*User; // 
   
	inserted, updated, err:= manager.PersistAll(&users, "users", nil)
	if err != nil {
       log.Fatal(err)
   	}

    
    err:= manager.ReadAll(&users, "SELECT id, name FROM users WHERE id IN(?, ?)", []interface{}{1, 10},nil)
	 if err != nil {
         log.Fatal(err)
     }

   
  
    deleted, err := manager.DeleteAll(&users, "users", nil)
    if err != nil {
        log.Fatal(err)
   	}
  
}
```

<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:**