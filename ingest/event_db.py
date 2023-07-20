from abc import ABC, abstractmethod
import speech_recognition as sr
from tqdm import tqdm

class EventDb(ABC):
    
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

    def upload(self, session):
            #upload the session with all the events to event database
            session_key = self._create_session(session.session_id)
            if session_key:
                print('Succesfully created session ' + session.session_id + ' in database')
            else: 
                print('There was an issue creating session ' + session.session_id + ' in database, Skipping')
                return
            for event in tqdm(session.events, desc="Creating events in database for session: " + session.session_id):
                event_key = self._create_event_in_session(session_key=session_key, event=event)
                if not event_key:
                    print('   [Error] There was an issue uploading event ' + event.audio_tag + ' in session')



