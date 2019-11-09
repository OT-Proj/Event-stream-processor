import json
import random
from datetime import datetime, timedelta


class RandomDataGenerator:
    def __init__(self):
        # because my local machine generates the events extremely fast,
        # all the generated session_id's are identical. In order to generate consistent data,
        # we will simulate time passage with a datetime object

        self.simulated_time = datetime.now()

    def generateData(self, output_file):
        # generates random event stream data

        data = []

        # dummy users and pages:

        # please note that some users open (non-concurrent) sessions.
        users = ["Pooh", "Piglet", "eeyore", "christopher robbin", "Pooh", "Piglet", "unlucky_user"]
        pages = ["/pageX", "/pageY", "/pageZ"]

        for user in users:
            # to generate a unique session ID, we join the userID,
            # a character that is invalid to appear in the userID,
            # and a timestamp that represents the session start time.
            # assuming users are not allowed to open multiple concurrent sessions at the same moment,
            # this will result a unique sessionID.

            session_id = user + "#" + str(self.simulated_time.timestamp())
            self.simulated_time += timedelta(seconds=1)

            # create session start
            url = random.choice(pages)
            data.append(self.generateEvent(user, "start_session", session_id, url))

            # since the "in_page" happens on EVERY url hit, we always append an "in_page" event
            data.append(self.generateEvent(user, "in_page", session_id, url))

            # generate random number of in_page events
            for i in range(0, random.randint(0, 5)):
                url = random.choice(pages)  # simulates the user browsing through pages
                data.append(self.generateEvent(user, "in_page", session_id, url))

            # generate conversion and end_session
            url = random.choice(pages)  # the user got to his final page
            data.append(self.generateEvent(user, "in_page", session_id, url))
            if user != "unlucky_user":
                # an unlucky user didn't manage to converse
                data.append(self.generateEvent(user, "conversion", session_id, url))
            data.append(self.generateEvent(user, "end_session", session_id, url))

        # write to JSON
        with open(output_file, "w") as f:
            json.dump(data, f, indent=4)

    def generateEvent(self, user_id, event_type, session_id, url):
        # generates a single event dictionary of a given type, for a given user.
        event = {}
        event["session_id"] = session_id
        event["user_id"] = user_id
        event["type"] = event_type
        event["url"] = url
        event["timestamp"] = self.simulated_time.timestamp()
        self.simulated_time += timedelta(seconds=1)

        return event


if __name__ == '__main__':
    generator = RandomDataGenerator()
    generator.generateData("data.json")
