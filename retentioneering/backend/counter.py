class Counter:
    def __init__(self) -> None:
        self.__event_index = 0
        self.__eventstream_index = 0

    def get_event_index(self) -> int:
        self.__event_index += 1
        return self.__event_index

    def get_eventstream_index(self) -> int:
        self.__eventstream_index += 1
        return self.__eventstream_index

    def reload(self) -> None:
        self.__event_index = 0
        self.__eventstream_index = 0


counter = Counter()
