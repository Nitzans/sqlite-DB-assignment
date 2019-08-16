## sqlite DB assignment
### Description 
The program contains two modules: sender and receiver, which connected by RabbitMQ (Message queue).  
The sender module sends a message which contains a database path, a country name and a year.  
The receiver module listens to the queue and performs four queries on the DB.  
The first output is written into a CSV file and stored as a new table in the DB.  
The second output is written into a CSV file and stored as a new table in the DB.  
The third output is written into a JSON file.  
The Fourth output is written into an XML file and stored as a new table in the DB.  

### Instruction  
1. Run Receiver.py  
2. Run Sender.py with the following arguments: <DB-PATH> <COUNTRY> <YEAR>  
you can run Sender.py many times with different parameters.  
