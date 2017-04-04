# dynamo

AWS dynamodb bindings for eventsource

## Example

```go
store, err := dynamo.New("user_events",
    dynamo.WithRegion("us-west-2"),
)
if err != nil {
    log.Fatalln(err)
}

repo := eventsource.New(&User{},
    eventsource.WithStore(store),
    eventsource.WithSerializer(eventsource.NewJSONSerializer(
        UserCreated{},
        UserNameSet{},
        UserEmailSet{},
    )),
)
```