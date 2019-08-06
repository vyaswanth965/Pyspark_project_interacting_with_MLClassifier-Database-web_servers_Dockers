from Levenshtein.StringMatcher import StringMatcher
import string
# from fuzzywuzzy import process,fuzz
import operator

class MatchString():

    def __init__(self):
        self.scores =[]
        self.replace_punctuation = str.maketrans(string.punctuation, ' '*len(string.punctuation))
        self.constant = 0

    def sortedSentence(self,Sentence):       
        # Spliting the Sentence into words 
        words = Sentence.split(" ")        
        # Sorting the words 
        words.sort()        
        # Making new Sentence by  
        # joining the sorted words 
        newSentence = " ".join(filter(None, words))        
        # Return newSentence 
        return newSentence 

    def clearCache(self):
        self.scores = []

    def getMatch(self,seq1, seq2):
        m = StringMatcher(seq1=seq2,seq2=seq1)
        ration = m.quick_ratio()
        ration = round(ration * 100,2)
        self.scores.append((seq2, ration))

    def getMatchidwithThreshold(self,seq1, seq2,id,score_cutoff=0):
        m = StringMatcher(seq1=seq2,seq2=seq1)
        ration = m.quick_ratio()
        ration = round(ration * 100,2)
        if ration >= score_cutoff:
            self.scores.append((id, ration))        

    def getMatchwithThreshold(self,seq1, seq2,score_cutoff=0):
        m = StringMatcher(seq1=seq2,seq2=seq1)
        ration = m.quick_ratio()
        ration = round(ration * 100,2)
        if ration >= score_cutoff:
            self.scores.append((seq2, ration)) 

    def sort_table(self,table, col=0):
        return sorted(table, key=operator.itemgetter(col), reverse=True)

    def extractBestMatches(self,target,options, limit,score_cutoff=0):
        self.clearCache()
        target = target.lower()
        target = target.translate(self.replace_punctuation)
        target = self.sortedSentence(target)
        score_cutoff = score_cutoff - self.constant

        for cleandesc,id in options:
            self.getMatchidwithThreshold(target,cleandesc,id,score_cutoff)
        return self.sort_table(self.scores, 1)[:limit]

    def extractOneWithcutoff(self,target,options, score_cutoff):
        self.clearCache()
        target = target.lower()
        target = target.translate(self.replace_punctuation)
        target = self.sortedSentence(target)
        score_cutoff = score_cutoff - self.constant
        for cleandesc in options:
            self.getMatchwithThreshold(target,cleandesc,score_cutoff)
        results = self.sort_table(self.scores, 1)
        if len(results)>0:    
            return results[0]
        else:
            return None    

    def extractOne(self,target,options):
        self.clearCache()
        target = target.lower()
        target = target.translate(self.replace_punctuation)
        target = self.sortedSentence(target)
        for option in options:
            self.getMatch(target,option)
        return self.sort_table(self.scores, 1)[0]