# Kafka Wrapper for Golang 

Please install the following dependencies:
1. **go** *get* github.com/bsm/sarama-cluster
2. **go** *get* github.com/Shopify/sarama

Both repositories function with the help of the GCC Compiler.

To resolve this on *Windows*, install MinGW-w64 >8.1.0. The easiest way to do this is with *Chocolatey*.

### Install MinGW with Choco
1. Open PowerShell
2. Copy and paste the command: 

    ```ruby
    C:\> Set-ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))
    ```
3. Copy and paste the command:
    ```ruby
    C:\> choco install mingw
    ```
## Functionality

Please view the **example.go** script to see how to use the wrapper in a simple way.

Apart from what is within the example script, it is also possible to instantiate a Consumer which belongs to a certain group.

```go
brokers  := []string{"0.0.0.0:1234", "0.0.0.0:4321"}
topics   := []string{"example-topic-1", "example-topic-2"}
group    := "example-group-1"

consumer := MakeGroupConsumer(brokers, topics, group)
consumer.GroupConsume()
```
*The **run.cmd** script runs the example together with the producers.*
   