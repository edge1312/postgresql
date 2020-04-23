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
            student_id integer PRIMARY KEY not null,
            name varchar(100) not null,
            gpa numeric(10,2),
            birth date
        );
    """)
    cur.execute("""
            CREATE TABLE if not exists Course(
            course_id integer PRIMARY KEY not null,
            course_name varchar(100) not null
        );
    """)
    cur.execute("""
            CREATE TABLE if not exists Student_to_Course(
            student_id integer references Student(student_id),
            course_id integer references Course(course_id),
            PRIMARY KEY (student_id, course_id)
        );
    """)

    # cur.execute("""
    #         CREATE TABLE if not exists Student_to_Course(
    #         id serial PRIMARY KEY,
    #         student_id integer references Student(student_id),
    #         course_id integer references Course(course_id)
    #     );
    # """)

    conn.commit()
    conn.close()


# создает таблицу курсов
def add_courses(courses):
    print('Adding courses...')
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            for course in courses:
                try:
                    cur.execute("""
                        INSERT INTO Course(course_id, course_name)
                        VALUES(%s, %s);
                    """, (course['id'], course['name']))
                except:
                    conn.rollback()


# выводит таблицу курсов
def get_courses():
    print('Getting courses...')
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
            """)
            rows = cur.fetchall()
        for row in rows:
            print(row)


# создает студентов и записывает их на курс
def add_students(course_id, student):
    print('Creating student and adding to course...')
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO Student(student_id, name, gpa, birth)
                    VALUES(%(id)s, %(name)s, %(gpa)s, %(birth)s);
                """, student)
            except:
                conn.rollback()
            try:
                cur.execute("""
                    INSERT INTO Student_to_Course(student_id, course_id)
                    VALUES (%s, %s)
                """, (student['id'], course_id))
            except:
                conn.rollback()


# выводит студентов определенного курса
def get_students(course_id):
    print('Getting students by course_id', course_id)
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.student_id, s.name, c.course_name from Student_to_Course sc
                JOIN Student s on s.student_id = sc.student_id
                JOIN Course c on c.course_id = sc.course_id
                WHERE c.course_id = %s;
            """, [course_id])
            students = cur.fetchall()
        for student in students:
            print(student)


# выводим всех студентов
def get_all_students():
    print('Getting all students...')
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
            """)
            students = cur.fetchall()
        for student in students:
            print(student)


def get_student_to_course():
    print('Getting students to courses...')
    with pg.connect(
            dbname='test_db',
            user='test',
            password='1234',
            host='localhost',
            port='5432'
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * from Student_to_Course
            """)
            rows = cur.fetchall()
            for row in rows:
                print('student_id:', row[0], 'course_id:', row[1])


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
            try:
                cur.execute("""
                    INSERT INTO Student(student_id, name, gpa, birth)
                    VALUES(%(id)s, %(name)s, %(gpa)s, %(birth)s);
                """, student)
            except:
                conn.rollback()


# получаем студента по id
def get_student(student_id):
    print('Getting student by student_id', student_id)
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
            WHERE student_id = %s;
            """, [student_id])
            current_student = cur.fetchone()
            print(current_student)




if __name__ == '__main__':

    create_db()

    courses = [
        {'id': 1, 'name': 'Python'},
        {'id': 2, 'name': 'PHP'},
        {'id': 3, 'name': 'Front-end'},
        {'id': 5, 'name': 'SQL'},
        {'id': 9, 'name': 'C++'}
    ]

    add_courses(courses)

    get_courses()


    rr = {'id': 1, 'name': 'Роман Романов', 'gpa': None, 'birth': 'December 13, 2003'}
    kk = {'id': 2, 'name': 'Кузьма Кузьмин', 'gpa': 3.95, 'birth': 'November 11, 2003'}
    pp = {'id': 3, 'name': 'Петр Петров', 'gpa': 4.11, 'birth': 'December 23, 2003'}
    ii = {'id': 4, 'name': 'Иван Иванов', 'gpa': None, 'birth': 'December 13, 2002'}
    nn = {'id': 5, 'name': 'Николай Николаев', 'gpa': 4.65, 'birth': 'November 11, 2002'}
    vv = {'id': 6, 'name': 'Всеволод Всеволодов', 'gpa': 4.15, 'birth': 'December 23, 2002'}


    add_students(1, rr)
    add_students(2, kk)
    add_students(2, pp)
    add_students(3, nn)

    get_all_students()

    get_student_to_course()

    add_student(vv)
    add_student(rr)

    get_all_students()

    get_student(1)
    get_student(4)

    get_students(2)
