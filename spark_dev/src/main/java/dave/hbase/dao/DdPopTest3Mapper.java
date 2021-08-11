package dave.hbase.dao;

import java.util.List;

import dave.hbase.domain.DdPopTest;
import dave.hbase.domain.DdPopTestExample;
import dave.hbase.domain.DdPopTestKey;
import org.apache.ibatis.annotations.Param;

public interface DdPopTest3Mapper {
    int countByExample(DdPopTestExample example);

    int deleteByExample(DdPopTestExample example);

    int deleteByPrimaryKey(DdPopTestKey key);

    int insert(DdPopTest record);

    int insertSelective(DdPopTest record);

    List<DdPopTest> selectByExample(DdPopTestExample example);

    DdPopTest selectByPrimaryKey(DdPopTestKey key);

    int updateByExampleSelective(@Param("record") DdPopTest record, @Param("example") DdPopTestExample example);

    int updateByExample(@Param("record") DdPopTest record, @Param("example") DdPopTestExample example);

    int updateByPrimaryKeySelective(DdPopTest record);

    int updateByPrimaryKey(DdPopTest record);
}