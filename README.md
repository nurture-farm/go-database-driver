# go-database-driver

## This library gives you the support to execute different database query engines, currently we have added support for (trino, presto, mysql, athena), all you have to do is following:- 


### Import this library in your project.


```
import "github.com/nurture-farm/go-database-driver"
import "github.com/nurture-farm/go-database-driver/driverConfig"
```

### Initialise the drivers

```
trinoConfig := &driverConfig.TrinoConfig{
    Dsn: viper.GetString("trino_url"),
}
drivers.InitializeDrivers(drivers.TRINO, trinoConfig)
```

### Call the function to execute the query with query and driver type

```
_, data, err := drivers.ExecuteQuery(base.QueryActiveActor, drivers.TRINO)
```
