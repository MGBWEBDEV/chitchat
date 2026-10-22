# Chit Chat

Requires Go 1.27 or newer. Run commands from the project root.

## Run

Start the server:

```powershell
go run ./server
```

In three separate terminals, start clients with different names:

```powershell
go run ./client -name Alice
go run ./client -name Bob``
go run ./client -name Carol
```

Type a message and press Enter to publish it. Type `/leave` to exit.


