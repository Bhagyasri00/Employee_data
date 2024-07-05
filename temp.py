# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import json
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import sys

def connect_to_db():
    connection = psycopg2.connect(user= 'postgres', password = '',
                                  host = 'localhost',
                                  port= 5432,
                                  database= 'employee_data')
    return connection
    

def table_counts():
    connection = connect_to_db()
    query = '''select count(*), 'department' as table_name from employees.department union all
    select count(*), 'employee' as table_name from employees.department_employee union all
    select count(*), 'manager' as table_name from employees.employee union all
    select count(*), 'salary' as table_name from employees.salary union all
    select count(*), 'title' as title from employees.title;'''
    

    count = pd.read_sql_query(query, connection)
    
    # Plot the data
    plt.figure(figsize=(10, 6))
    sns.barplot(x='table_name', y='count', data=count, palette='viridis')
    plt.title('Counts in Each Table')
    plt.xlabel('Table Name')
    plt.ylabel('Count')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()
    
    return count
 
#print(table_counts())

def avg_salary_per_department():
    connection = connect_to_db()
    query = '''select d1.dept_name as Department , round(avg(s.amount),2) as avg_salary
    from employees.department d1
    left join employees.department_employee d2
    on d1.id = d2.department_id
    left join employees.salary s
    on d2.employee_id = s.employee_id
    where date_part('year',d2.to_date) = 9999 and date_part('year', s.to_date) = 9999
    group by d1.dept_name
    order by avg_salary DESC'''
    
    avg_salary = pd.read_sql_query(query, connection)
    
    highest_salary_dept = avg_salary.loc[avg_salary['avg_salary'].idxmax()]['department']
    print(highest_salary_dept)

    
    #plot the data
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='department', y='avg_salary', data=avg_salary, color='Black')
    plt.scatter(avg_salary['department'], avg_salary['avg_salary'], color='red', marker='D', s=50)
    plt.bar(avg_salary['department'], avg_salary['avg_salary'], color='pink', edgecolor='black')
    plt.title('Average Salary Distribution by Department')
    plt.xlabel('Department')
    plt.ylabel('Average Salary')
    plt.xticks(rotation=45)    
     
#print(avg_salary_per_department())    


def avg_salary_per_title():
    connection= connect_to_db()
    query = '''select t.title as Designation, round(avg(s.amount),2) as Avg_salary from employees.title t 
    left join employees.salary s
    on t.employee_id = s.employee_id
    group by t.title
    order by Avg_salary DESC'''
    
    avg_salary = pd.read_sql_query(query, connection)
    highest_salary_dept = avg_salary.loc[avg_salary['avg_salary'].idxmax()]['designation']
    print(highest_salary_dept)
    

    
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='designation', y='avg_salary', data=avg_salary, color='Black')
    plt.scatter(avg_salary['designation'], avg_salary['avg_salary'], color='Blue', marker='o', s=50)
    plt.bar(avg_salary['designation'], avg_salary['avg_salary'], color='lavender', edgecolor='black')
    plt.title('Average Salary Distribution by Department')
    plt.xlabel('Designation')
    plt.ylabel('Average Salary')
    plt.xticks(rotation=45)  
    
    
#print(avg_salary_per_title())

def depart_salary_by_gender():
    connection= connect_to_db()
    query = '''select gender, amount , dept_name
                from employees.employee e
				join  employees.salary  s on  e.id=s.employee_id
				join  employees.department_employee de on de.employee_id= s.employee_id
				join employees.department d on de.department_id = d.id'''
                
    df= pd.read_sql_query(query, connection)
    grouped_df = df.groupby(['gender', 'dept_name'])['amount'].mean().reset_index()
  
    pivot_df = grouped_df.pivot(index='dept_name', columns='gender', values='amount')
    
    # Plotting
    pivot_df.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='coolwarm')
    plt.title('Mean Salary by Gender and Department (Stacked)')
    plt.xlabel('Department')
    plt.ylabel('Mean Salary')
    plt.xticks(rotation=45)
    plt.legend(title='Gender')
    plt.tight_layout()
    plt.show()
    
    return pivot_df


#print(depart_salary_by_gender())          

def title_count_by_gender():
    connection = connect_to_db()
    query = '''
        SELECT gender, title, COUNT(title) AS count_title
        FROM employees.employee e 
        JOIN employees.title t ON e.id = t.employee_id
        WHERE DATE_PART('year', t.to_date) = 9999
        GROUP BY title, gender
    '''
    df = pd.read_sql_query(query, connection)
    connection.close()

    pivot_df = df.pivot(index='title', columns='gender', values='count_title')

    
    pivot_df.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='coolwarm')
    plt.title('Count of Employees by Title and Gender ')
    plt.xlabel('Title')
    plt.ylabel('Count of Employees')
    plt.xticks(rotation=45)
    plt.legend(title='Gender')
    plt.tight_layout()
    plt.show()

    return pivot_df

#title_count_by_gender()

    
def title_salary_distribution():
    connection= connect_to_db()
    query= '''select t.title as Designation, s.amount
from employees.salary s
LEFT JOIN employees.department_employee d on s.employee_id = d.employee_id 
left join employees.title t on t.employee_id = s.employee_id
WHERE date_part('year', s.to_date) = 9999  
AND date_part('year', d.to_date) = 9999'''

    title_salary = pd.read_sql_query(query, connection)
    
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=title_salary, x='amount', y='designation', palette= 'vlag')
    plt.title('Salary Distribution across Title')
    plt.show()
    
    return title_salary
      
#print(title_salary_distribution())

def department_salary_distribution():
    connection= connect_to_db()
    query= '''SELECT d.dept_name, s.amount  
FROM employees.salary s 
LEFT JOIN employees.department_employee de ON s.employee_id = de.employee_id 
LEFT JOIN employees.department d ON d.id = de.department_id 
WHERE date_part('year', s.to_date) = 9999  
AND date_part('year', de.to_date) = 9999'''

    department_salary= pd.read_sql_query(query, connection)
     
    plt.figure(figsize=(10, 6))
    sns.violinplot(data=department_salary, x='amount', y='dept_name', palette= 'YlOrBr')
    plt.title('Salary Distribution across department')
    plt.show()
     
    return department_salary
 
#print(department_salary_distribution()) 

         
def department_active_managers():
    connection= connect_to_db()
    query = '''select d1.dept_name, count(distinct d2.department_id) as manager_count
from employees.department d1
left join employees.department_employee d2
on d1.id = d2.department_id
LEFT JOIN employees.title t ON d2.employee_id = t.employee_id
where date_part('year', d2.to_date) = 9999 
AND t.title = 'Manager'  
group by d1.dept_name'''
     
    active_managers_per_department = pd.read_sql_query(query, connection)
    
    
#print(department_active_managers())

def title_composition_by_department():
    connection= connect_to_db()
    query = '''SELECT d.dept_name, t.title, COUNT(*) AS title_count
FROM employees.department d
LEFT JOIN employees.department_employee de ON d.id = de.department_id
LEFT JOIN employees.title t ON de.employee_id = t.employee_id
WHERE date_part('year', de.to_date) = 9999
GROUP BY d.dept_name, t.title'''
    
    department_counts_df = pd.read_sql_query(query, connection)
    
    # Get unique departments
    unique_departments = department_counts_df['dept_name'].unique()
    
    # Define a color palette using seaborn
    colors = sns.color_palette("husl", n_colors=len(unique_departments))
    
    for i, department in enumerate(unique_departments):
        department_data = department_counts_df[department_counts_df['dept_name'] == department]
        
        plt.figure(figsize=(8, 8))
        plt.pie(department_data['title_count'], labels=department_data['title'], autopct='%1.1f%%', startangle=180, colors=colors)
        plt.title(f'Title Composition for Department: {department}')
        plt.axis('equal') 
        plt.legend(department_data['title'] ,bbox_to_anchor=(1.02, 1), loc='upper left')
        plt.tight_layout()
        plt.show()

    return department_counts_df

#print(title_composition_by_department())

def department_composition_by_title():
    connection= connect_to_db()
    query = '''SELECT d.dept_name, t.title, COUNT(*) AS title_count
FROM employees.department d
LEFT JOIN employees.department_employee de ON d.id = de.department_id
LEFT JOIN employees.title t ON de.employee_id = t.employee_id
WHERE date_part('year', de.to_date) = 9999
GROUP BY d.dept_name, t.title'''
    
    department_counts_df = pd.read_sql_query(query, connection)
    
    unique_titles = department_counts_df['title'].unique()
    
    # Define a color palette using seaborn
    colors = sns.color_palette('Accent', n_colors=len(unique_titles))
    
    for title in unique_titles:
        title_data = department_counts_df[department_counts_df['title'] == title]
        
        plt.figure(figsize=(12,10))
        plt.pie(title_data['title_count'], labels=title_data['dept_name'], autopct='%1.1f%%', startangle=180, colors=colors)
        plt.title(f'Department Composition for Title: {title}')
        plt.legend(title_data['dept_name'], bbox_to_anchor=(1.02, 1), loc='upper left')
        plt.axis('equal')
        plt.show()

    return department_counts_df


#print(department_composition_by_title())


#def salaries_of_active_managers():
    connection = connect_to_db()
    query = '''
        SELECT d.dept_name, s.amount
        FROM employees.salary s
        LEFT JOIN employees.department_manager dm ON dm.employee_id = s.employee_id
        LEFT JOIN employees.department d ON d.id = dm.department_id
        WHERE date_part('year', dm.to_date) = 9999
        AND date_part('year', s.to_date) = 9999
        order by amount DESC
    '''
    
    # Execute SQL query and fetch data into a DataFrame
    df = pd.read_sql_query(query, connection)
    custom_palette = ['#FF5733', '#FFBD33', '#FF3399', '#33FF57', '#3366FF']

    # Plot the salaries using a violin plot
    plt.figure(figsize=(10, 6))
    sns.barplot(x='amount', y='dept_name', data=df, ci='sd', capsize=0.4, errcolor='.5', edgecolor='.5', linewidth=2.5, palette=custom_palette, orient='h')
    plt.title('Salaries of Active Department Managers')
    plt.xlabel('Salary')
    plt.ylabel('Department')
    plt.show()
    
    # Find the department with the highest salary
    highest_salary_dept = df.loc[df['amount'].idxmax()]['dept_name']
    print(f"The department with the highest salary for an active manager is: {highest_salary_dept}")


#salaries_of_active_managers()

def active_manager_title():
    connection = connect_to_db()
    query = '''select distinct t.title 
from employees.title t 
left join employees.department_manager dm on dm.employee_id = t.employee_id
WHERE date_part('year', dm.to_date) = 9999 and date_part('year', t.to_date) = 9999'''
    df = pd.read_sql_query(query, connection)
    
    return df

#active_manager_title()

def yearly_salary_dept_wise():
    connection = connect_to_db()
    query = """SELECT dm.employee_id, d.dept_name, s.amount, s.from_date, s.to_date FROM employees.department_manager dm join employees.salary s 
              on dm.employee_id = s.employee_id join employees.department d
			  on d.id = dm.department_id
			  where s.from_date >= dm.from_date
			  and s.to_date <= dm.to_date"""
    df= pd.read_sql_query(query, connection)
    df['labels'] = df['from_date'].apply(str) + " to " + df['to_date'].apply(str)
    depts= df['dept_name'].unique()
    employees_manager = df['employee_id'].unique()
    for i in depts:
        for j in employees_manager:
            new_df= df[(df['dept_name']==i) & (df['employee_id']==j)]
            if not new_df.empty:
                my_plot= sns.barplot(x= new_df['labels'], y= new_df['amount'], palette='pastel')
                for item in my_plot.get_xticklabels():
                    item.set_rotation(90)
                plt.title(i+str(j))
                plt.show()
            else:
                continue
    
#yearly_salary_dept_wise()

    
def get_median_salry_inc_dept_wise():
    connection = connect_to_db()
    query_get_all_depts= "select distinct(dept_name) from employees.department;"
    all_depts = pd.read_sql_query(query_get_all_depts, connection)['dept_name'].tolist()
    
    median_inc= {}
    for i in all_depts:
        query = """ select s.amount, d.dept_name, date_part('year', s.from_date) as start, 
                date_part('year', s.to_date) as end from employees.salary s join  employees.department_employee de 
				on s.employee_id = de.employee_id join employees.department d on d.id =de.department_id 
				where dept_name = '%s' order by s.from_date desc"""
        df = pd.read_sql_query(query %i, connection)
        df['start'] = df['start'].astype('int')
        df['end'] = df['end'].astype('int')
        #my_df= df.groupby(['start','end'])['amount'].median().diff()
        my_df= df.groupby(['start'])['amount'].mean().diff()
        my_df.dropna(inplace= True)
        plot= sns.barplot(x= my_df.index, y = my_df.values)
        for item in plot.get_xticklabels():
            item.set_rotation(90)
        median_inc[i] = np.median(my_df.values)
        plt.title(i)
        plt.show()
    return median_inc

#get_median_salry_inc_dept_wise()

def salary_by_work_experience():
    connection = connect_to_db()
    query = '''
        SELECT amount,
               CASE
                   WHEN work_ex > 10 THEN 'More than 10 years'
                   WHEN work_ex BETWEEN 4 AND 10 THEN 'More than 4 years'
                   WHEN work_ex BETWEEN 1 AND 4 THEN 'More than 1 year'
                   ELSE 'Fresher'
               END AS work_exp
        FROM (
            SELECT s.amount,
                   DATE_PART('year', de.to_date) - DATE_PART('year', de.from_date) AS work_ex
            FROM employees.department_employee de
            JOIN employees.salary s ON s.employee_id = de.employee_id
            WHERE DATE_PART('year', de.to_date) - DATE_PART('year', de.from_date) <= 60
              AND DATE_PART('year', s.to_date) = 9999
        ) AS r
    '''
    df = pd.read_sql_query(query, connection)

    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='work_exp', y='amount', data=df, estimator=sum, ci=None, palette='coolwarm')
    plt.title('Total Salary by Work Experience')
    plt.xlabel('Work Experience')
    plt.ylabel('Total Salary')
    plt.tight_layout()
    plt.show()

    return df

def work_experience_by_title():
    connection = connect_to_db()
    query = '''
        SELECT t.title,
               DATE_PART('year', de.to_date) - DATE_PART('year', de.from_date) AS work_ex
        FROM employees.department_employee de
        JOIN employees.title t ON de.employee_id = t.employee_id
        WHERE DATE_PART('year', de.to_date) != 9999
    '''
    df = pd.read_sql_query(query, connection)
    connection.close()

    
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='title', y='work_ex', data=df, estimator=sum, ci=None, palette='coolwarm')
    plt.title('Total Work Experience by Title')
    plt.xlabel('Title')
    plt.ylabel('Total Work Experience')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


    return df

#work_experience_by_title()

def work_experience_by_department():
    connection = connect_to_db()
    query = '''
        SELECT d.dept_name,
               DATE_PART('year', de.to_date) - DATE_PART('year', de.from_date) AS work_ex
        FROM employees.department_employee de
        JOIN employees.department d ON de.department_id = d.id
        WHERE DATE_PART('year', de.to_date) != 9999
    '''
    df = pd.read_sql_query(query, connection)
    connection.close()

    
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(x='dept_name', y='work_ex', data=df, estimator=sum, ci=None, palette='Pastel1')
    plt.title('Total Work Experience by Department')
    plt.xlabel('Department')
    plt.ylabel('Total Work Experience')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    return df

work_experience_by_department()



    
    
    
    

    

    
