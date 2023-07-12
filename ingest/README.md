# Ingest Job
The ingest python project provides an interface with the ingest pipeline allowing to: 
- Retrieve sessions from the provided AWS bucket
- Check sessions against existing ones in the provided database
- Retrive metadata (and eventually raw data) and save it locally
- Upload data to the provided database

## Event Database
Event Database (`EventDb`) is an abstract class to be used for the implementations of the destination database for the Sessions and the Events. 

Any other Event Database inheriting from `EventDb` should implement the following methods: 

```
    @abstractmethod
    def _create_session(self, session_id):
        pass

    @abstractmethod
    def _create_event_in_session(self, session_id, event):
        pass

    @abstractmethod
    def session_exists(self, session_id):
        pass
    
    @abstractmethod
    def connect(self):
        pass
```
