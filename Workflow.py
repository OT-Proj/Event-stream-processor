from EventStreamProcessor import *


class Workflow:
    # this class executes batches of EventStreamProcessor queries
    def __init__(self):
        self.esp = EventStreamProcessor(r"data\data.json")

    def ExecuteAllQueries(self):
        # ex1
        self.esp.top10Convertors(r"results\top10Convertors")

        # ex2
        self.esp.fastConvertingUsers(r"results\fastConvertingUsers")

        # ex3
        self.esp.avgConversionDistance(r"results\avgConversionDistance")

        # something extra
        self.esp.unsuccessfulConversionRatio(r"results\unsuccessfulConversionRatio")

        # bonus ex
        pattern = ["/pageY", "/pageX"]
        self.esp.patternRecognition(r"results\patternRecognition", pattern)


if __name__ == '__main__':
    wf = Workflow()
    wf.ExecuteAllQueries()
