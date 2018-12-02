import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: hadoop-join
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-02 01:58
 **/
public class Emp_Dep implements Writable {

    private String name;
    private String sex;
    private int age;
    private int depNo;
    private String depName;

    public Emp_Dep(String name, String sex, int age, int depNo, String depName) {
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.depNo = depNo;
        this.depName = depName;
    }

    public Emp_Dep() {
    }

    // 实现Writable接口中的序列化方法
    // 方法的功能就是将对象中的属性数据转成二进制码写入输出流
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeUTF(this.sex);
        out.writeInt(this.age);
        out.writeInt(this.depNo);
        out.writeUTF(this.depName);
    }

    // 实现Writable接口中的反序列化方法
    // 方法的功能就是从输入流中读取二进制码并恢复成正确类型的数据并赋值给对象属性变量
    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.sex = in.readUTF();
        this.age = in.readInt();
        this.depNo = in.readInt();
        this.depName = in.readUTF();
    }


    public void set(String name, String sex, int age, int depNo, String depName) {
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.depNo = depNo;
        this.depName = depName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getDepNo() {
        return depNo;
    }

    public void setDepNo(int depNo) {
        this.depNo = depNo;
    }

    public String getDepName() {
        return depName;
    }

    public void setDepName(String depName) {
        this.depName = depName;
    }

    @Override
    public String toString() {
        return "Emp_Dep{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                ", depNo=" + depNo +
                ", depName='" + depName + '\'' +
                '}';
    }
}
