from heapq import *
from datetime import datetime
import sys
# global dictinaries to store data with key as (CMTE_ID , ZipCode) and (CMTE_ID,Transaction_Date)
dictByZipCode = {}
dictByDate = {}


# class defined to store transaction_amt in heap ,
# so that running median can be retrieved in O(1) and new amount value corresponding to unique key be added in O(log(n))
class medianCalculate(object):

    def __init__(self):
        self.heaps = [], []
#This methods add new amt to heap
    def addAmtInHeap(self, amt):
        small, large = self.heaps
        heappush(small, -heappushpop(large, amt))
        if len(large) < len(small):
            heappush(large, -heappop(small))
#This method retrieves median from heap
    def findMedian(self):
        small, large = self.heaps
        if len(large) > len(small):
            return float(large[0])
        return (large[0] - small[0]) / 2.0
#This class contains methods that apply necessary business logics on input lines , add data to global dictionaries that hold data related to every transaction.
class addAndProcessData(object) :
#this method data by considered combination of CMPTE_ID and ZipCode in the global dictionary and also call method to write line to outline line
    @staticmethod
    def addZipData(data,filepath):
        #data[10] is Zipcode , data[0] is CMPTE_ID , data[14] is Transaction_Amt
        # conditions to check the proper format of zipcode , transaction_amt and cmpte_id
        if (len(data[10]) < 5) or (len(data[0]) == 0) or (len(data[14]) == 0):
            pass
        else:
            key = data[0] + data[10][:5]
            amt = int(data[14])
            if key in dictByZipCode:
                dictByZipCode[key][3] = dictByZipCode[key][3] + amt
                dictByZipCode[key][5] += 1
                dictByZipCode[key][6].addAmtInHeap(amt)
                outputLine = (
                    dictByZipCode[key][0] + "|" + dictByZipCode[key][1] + "|" + str(int(round(dictByZipCode[key][6].findMedian()))) + "|" + str(
                        dictByZipCode[key][5]) + "|" +
                str(dictByZipCode[key][3]) + "\n")
                addAndProcessData.storeInFile(filepath+"medianvals_by_zip.txt",outputLine)

            else:
                medianData = medianCalculate()
                medianData.addAmtInHeap(int(data[14]))
                value = [data[0], data[10][:5], data[13], int(data[14]), data[15], 1, medianData]
                dictByZipCode[key] = value
                outputLine = (
                    dictByZipCode[key][0] + "|" + dictByZipCode[key][1] + "|" + str(
                        int(round(dictByZipCode[key][6].findMedian()))) + "|" + str(
                        dictByZipCode[key][5]) + "|" +
                    str(dictByZipCode[key][3]) + "\n")
                addAndProcessData.storeInFile(filepath+"medianvals_by_zip.txt",outputLine)
# method to build dictByDate for every combination of CMPTE_ID and Date with applying necessary  logics and prechecks.
    @staticmethod
    def addDateData(data):

# the conditions checks the proper date format (mmddyyyy) and if amt and cmpte_id is empty or not
        if addAndProcessData.dateFormatCheck(data[13]) or (len(data[0]) == 0) or (len(data[14]) == 0):


            pass
        else:
            key = data[0] + data[13]
            amt = int(data[14])
            if key in dictByDate:
                dictByDate[key][3] = dictByDate[key][3] + amt
                dictByDate[key][5] += 1
                dictByDate[key][6].addAmtInHeap(amt)

            else:
                medianData = medianCalculate()
                medianData.addAmtInHeap(int(data[14]))
                value = [data[0], data[10][:5], datetime.strptime(data[13], '%m%d%Y'), int(data[14]), data[15], 1, medianData]
                dictByDate[key] = value

#This method stores line in output file
    @staticmethod
    def storeInFile(fileName,dataLine):


        file = open(fileName,'a')
        file.write(dataLine)

        file.close()
#This method checks the input line date format
    @staticmethod
    def dateFormatCheck(d):
        try:
            datetime.strptime(d, '%m%d%Y')
            return False
        except ValueError:
            return True
# This method sorts the data in dictByDate dictionary with key (CMPTE_ID , Date) and call method to store line in medianvals_by_date.txt
    @staticmethod
    def sortAndPrintDataByData(outfilePath):
        sortedByDateKeys = sorted(dictByDate, key=lambda k: (dictByDate[k][0], dictByDate[k][2]))

        for key in sortedByDateKeys:
            outputLine = (dictByDate[key][0] + "|" + (datetime.date(dictByDate[key][2]).strftime("%m%d%Y")) + "|" + str(
                int(round(dictByDate[key][6].findMedian()))) + "|"
                          + str(dictByDate[key][5]) + "|" + str(dictByDate[key][3]) + "\n")

            addAndProcessData.storeInFile(outfilePath+"medianvals_by_date.txt", outputLine)




def main(argv):
    inputFilePath = argv[1]
    outputFilePath = argv[2]
    with open(inputFilePath) as f:
       for line in f:
           data = line.split("|")
#condition to check other_id field

           if  data[15]  :
               continue

           else :

               addAndProcessData.addZipData(data,outputFilePath)
               addAndProcessData.addDateData(data)
       addAndProcessData.sortAndPrintDataByData(outputFilePath)

if __name__ == "__main__":
    main(sys.argv)

