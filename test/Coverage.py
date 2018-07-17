
import os


def test_ExtractionGeocatalogueController():
    print("\n ------------------------ Coverage ExtractionGeocatalogueController ------------- \n")
    os.chdir('../src')
    os.system("coverage run ExtractionGeocatalogueController.py ")
    os.system("coverage report -m --omit  *backports_abc.py,GlobalData.py,Log.py,Source.py  >> ../test/coverageTest")
    os.chdir('../test')


def test_GlobalData():
    print("\n ------------------------ Coverage GlobalData ------------- \n")
    os.chdir('../src')
    os.system("coverage run GlobalData.py ")
    os.system("coverage report -m --omit  Log.py,Source.py >> ../test/coverageTest ")
    os.chdir('../test')


def test_Insertion():
    print("\n ------------------------ Coverage Insertion ------------- \n")
    os.chdir('../src')
    os.system("coverage run Insertion.py ")
    os.system("coverage report -m --omit *backports_abc.py,*sqlalchemy*,Declarative.py,GlobalData.py,Log.py,Source.py >> "
              "../test/coverageTest ")
    os.chdir('../test')


def test_Log():
    print("\n ------------------------ Coverage Log ------------- \n")
    os.chdir('../src')
    os.system("coverage run Log.py ")
    os.system("coverage report -m --omit GlobalData.py,Source.py >> ../test/coverageTest ")
    os.chdir('../test')


def test_VerificationMetadata():
    print("\n ------------------------ Coverage VerificationMetadata  ------------- \n")
    os.chdir('../src')
    os.system("coverage run VerificationMetadata.py ")
    os.system("coverage report -m --omit *backports_abc.py,*sqlalchemy*,Declarative.py,GlobalData.py,Log.py,Source.py >> "
              "../test/coverageTest ")
    os.chdir('../test')











if __name__ == "__main__":
    file = open('coverageTest','w+')

    test_ExtractionGeocatalogueController()
    test_GlobalData()
    test_Insertion()
    test_Log()
    test_VerificationMetadata()
    print '\n ------------------------ Result coverage--------------------\n'
    print file.read()
    file.close()