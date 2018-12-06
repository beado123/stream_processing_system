# Crane: a stream processing system

 ## To Start Nimbus
 ```
 go run Nimbus.go
 ```
 ## To Start other machines
 ```
 go build client.go
 ./client <id_of_nimbus>
 ```
 ## After runing server/client
 ## To print the membership list
 ```
 LIST
 ```
 ## To print self id
 ```
 SELF
 ```
 ## To join the group
 ```
 JOIN
 ```
 ## To leave the group
 ```
 LEAVE
 ```
 ## To fail current machine
 ```
 CTRL+C
 ```
  ## Operations in MP4
  # On Any machine other than Nimbus
  ## send topology to Nimbus
  ```
  <app_name> <num_of_worker>
  ```
