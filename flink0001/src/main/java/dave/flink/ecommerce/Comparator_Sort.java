package dave.flink.ecommerce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Comparator_Sort {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        ArrayList<Student> al = new ArrayList<Student>();
        al.add(new Student(5978,"Vishnu", 50));
        al.add(new Student(5979,"Vasanth", 30));
        al.add(new Student(5980,"Santhosh", 40));
        al.add(new Student(5981,"Santhosh", 20));
        al.add(new Student(5982,"Santhosh", 10));
        al.add(new Student(5983,"Santhosh", 5));


        Collections.sort(al, new AgeComparator());

        for(Student s : al){
            System.out.println(s.rollNo+" "+s.name+" "+s.age);
        }

    }


}

 class Student {

    int rollNo;
    String name;
    int age;

    public Student(int RollNo, String Name, int Age){
        this.rollNo = RollNo;
        this.name = Name;
        this.age = Age;
    }
}

 class AgeComparator implements Comparator<Student> {

    @Override
    public int compare(Student o1, Student o2) {
        return o1.age > o2.age ? 1 :(o1.age < o2.age ? -1 : 0); //Ascending

        //return o1.age < o2.age ? -1 :(o1.age > o2.age ? 1 : 0); // Descending
    }

}

