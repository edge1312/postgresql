import psycopg2 as pg

def create_db(): # создает таблицы

    conn = pg.connect(
        dbname='test_db',
        user='test',
        password='1234',
        host='localhost',
        port='5432'
    )

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE if not exists Student(
        id serial PRIMARY KEY,
        name varchar(100) not null,
        gpa numeric(10,2),
        birth date
        );
    """)
    cur.execute("""
        CREATE TABLE if not exists Course(
        id integer not null,
        name varchar(100) not null
        );
    """)

    conn.commit()
    conn.close()


# возвращает студентов определенного курса
def get_students(course_id):
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * from Course
            WHERE id = %s;
            """, [course_id])
            students = cur.fetchall()
        return students


# создает студентов и записывает их на курс
def add_students(course_id, students):
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO Student(name, gpa, birth)
            VALUES(%(name)s, %(gpa)s, %(birth)s);
            """, students)
            # здесь не додумался, как сделать через executemany, поэтому сделал через цикл for:
            for student in students:
                cur.execute("""
                INSERT INTO Course(id, name)
                VALUES(%s, %s);
                """, (course_id, student.get('name')))


# просто создает студента
def add_student(student):
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO Student(name, gpa, birth)
            VALUES(%(name)s, %(gpa)s, %(birth)s);""", student)


# выводим таблицу студентов
def print_table_student():
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * from Student;
            """)
            students = cur.fetchall()
            for row in students:
                print(row)


# просто выводим таблицу Course
def print_table_course():
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * from Course;
            """)
            courses = cur.fetchall()
            for row in courses:
                print(row)


# получаем студента по id
def get_student(student_id):
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * from Student
            WHERE id = %s;
            """, [student_id])
            current_student = cur.fetchone()
            print(current_student)


# удаляем студента
def delete_student(student_id):
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE from Student
            WHERE id = %s;
            """, [student_id])


if __name__ == '__main__':
    create_db()
    #delete_student(1)
    #get_student(1)


    students_course_1 = [
        {'name': 'Роман Романов', 'gpa': None, 'birth': 'December 13, 2003'},
        {'name': 'Кузьма Кузьмин', 'gpa': 3.95, 'birth': 'November 11, 2003'},
        {'name': 'Петр Петров', 'gpa': 4.11, 'birth': 'December 23, 2003'}
    ]
    students_course_2 = [
        {'name': 'Иван Иванов', 'gpa': None, 'birth': 'December 13, 2002'},
        {'name': 'Николай Николаев', 'gpa': 4.65, 'birth': 'November 11, 2002'},
        {'name': 'Всеволод Всеволодов', 'gpa': 4.15, 'birth': 'December 23, 2002'}
    ]

    add_students(1, students_course_1)
    add_students(2, students_course_2)

    # add_student({'name': 'Василий Васильев', 'gpa': 4.7, 'birth': 'January 8, 1999'})

    # print_table_student()
    # print_table_course()

    course_id = 2
    print(f'Студенты {course_id}-го курса:')
    for student in get_students(course_id):
        print(student[1])

