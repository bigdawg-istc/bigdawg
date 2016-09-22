#include "../src/data-formatter/buffer.h"

#include "gtest/gtest.h"


TEST(BufferTest,basic)
{
  std::cout << "Testing buffer" << std::endl;
  Buffer buffer;
  FILE * file;
  const char* fileName="testbuffer.bin";
  if((file=fopen(fileName,"w"))==NULL)
    {
      fprintf(stderr,"file %s could not be opened for writing\n",fileName);
      // just fail the test
      ASSERT_FALSE(1==1);
    }
    int data1 = 1;
    int data2 = 2;
    double data3 = 2.1;
    fwrite(&data1,sizeof(data1),1,file);
    fwrite(&data2,sizeof(data2),1,file);
    fwrite(&data3,sizeof(data3),1,file);
    fclose(file);

    if((file=fopen(fileName,"r"))==NULL)
    {
      fprintf(stderr,"file %s could not be opened for reading\n",fileName);
      // just fail the test
      ASSERT_FALSE(1==1);
    }

    BufferNew(&buffer,file,10);
    int returnedData1;
    int returnedData2;
    double returnedData3;

    size_t bytesRead1 = BufferRead(&returnedData1,sizeof(int),1,&buffer);
    ASSERT_EQ(bytesRead1,sizeof(int));
    ASSERT_EQ(data1,returnedData1);
    
    size_t bytesRead2 = BufferRead(&returnedData2,sizeof(int),1,&buffer);
    ASSERT_EQ(bytesRead2,sizeof(int));
    ASSERT_EQ(data2,returnedData2);

    size_t bytesRead3 = BufferRead(&returnedData3,sizeof(double),1,&buffer);
    ASSERT_EQ(bytesRead3,sizeof(double));
    ASSERT_EQ(data3,returnedData3);

    BufferDispose(&buffer);
    fclose(file);
}
