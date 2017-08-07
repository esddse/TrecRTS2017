import datetime
import time
from trecjson import TrecJson

class AdvancedTweet(TrecJson):
    def __init__(self, created_at, id_str, word_list_str, stem_list_str):
        TrecJson.__init__(self, set())

        self.created_at   = created_at
        self.timestamp    = time.mktime(datetime.datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y").timetuple())
        self.id_str       = id_str

        self.id           = self.id_str
        
        self.word_list    = word_list_str.split(' ')
        self.word_distri  = self.extract_distribution(self.word_list)
        self.word_tf      = self.extract_tf(self.word_list)
        self.stem_list    = stem_list_str.split(' ')
        self.stem_distri  = self.extract_distribution(self.stem_list)
        self.stem_tf      = self.extract_tf(self.stem_list)



