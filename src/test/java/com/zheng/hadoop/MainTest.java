package com.zheng.hadoop;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhenglian on 2017/11/10.
 */
public class MainTest {
    
    static class Person {
        private String name;
        
        public Person(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
    
    
    
    public static void main(String[] args) {

        List<Person> persons = new ArrayList<>();
        persons.add(new Person("zhang1"));
        persons.add(new Person("lisi1"));
        persons.add(new Person("lisi2"));
        persons.add(new Person("lisi3"));
        
        List<Person> lisis = new ArrayList<>();
        
        Person zhangsan = null;
        for (Person person : persons) {
            if (person.getName().startsWith("zhang")) {
                zhangsan = person;
            } else {
                lisis.add(person);
            }
        }
        
        
        
        
        System.out.println(zhangsan);
        System.out.println(lisis);
        
        
    }
    
    
}
