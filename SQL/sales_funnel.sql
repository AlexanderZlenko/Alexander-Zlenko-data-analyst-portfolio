-- sales_funnel 
WITH jsp AS (
-- getting information about payment description 
    SELECT 
    	client_id,
        id AS payment_id,
        explode(from_json(data, 'array<struct<Description:string>>')) AS data
    FROM raw_pg_services_distance_learning_public.payments
), 
jslc AS (
-- getting information about number of purchased lessons in a package
    SELECT 
    	client_id,
        payment_id,
        explode(FROM_json(data, 'array<struct<lessons_count:string>>')) AS lesson_count
    FROM raw_pg_services_distance_learning_public.orders
), 
all_payments AS (
-- all successful payments before 'united balance' feature was released 
-- this feature affects significantly on a project backend logic
-- that is the reason why we make some calculations in different way before and after that release (2021-08-27)
    SELECT 
    	p.client_id,
        p.moment,
        CASE
			when jsp.data['Description'] like '%%англ%%' then 1
           	when jsp.data['Description'] like '%%математ%%' then 2
        END AS subject,
		p.amount
    FROM raw_pg_services_distance_learning_public.payments AS p
	JOIN raw_pg_services_distance_learning_public.orders AS o ON o.payment_id = p.id
	JOIN jsp ON jsp.payment_id = p.id
    WHERE 1=1 
    AND (o.state = 'paid' or o.state = 'cancelled')
	AND jsp.data['Description'] NOT LIKE 'Пробное занятие%'
	AND COALESCE(p.testing, FALSE) = FALSE 
	AND (jsp.data['Description'] LIKE '%математ%' OR jsp.data['Description'] LIKE '%английск%')
	AND p.amount > 1
	AND to_date(p.moment) < '2021-08-27'
), 
new_payments as (
-- all successful payments after 'united balance' feature was released
    SELECT 
    	p.client_id,
        p.id,
        p.amount,
        to_date(p.moment) as moment,
        p.data,
        o.data,
        jslc.lesson_count['lessons_count'] AS lesson_count,
        lead(p.moment, 1) OVER (PARTITION BY p.client_id ORDER BY p.moment) AS next_payment_moment
    FROM raw_pg_services_distance_learning_public.payments AS p
	JOIN raw_pg_services_distance_learning_public.orders AS o ON o.payment_id = p.id
	JOIN jsp ON jsp.payment_id = p.id
	JOIN jslc ON jslc.payment_id = p.id
    WHERE 1=1
    AND o.state = 'paid'
	AND jsp.data['Description'] NOT LIKE 'Пробное занятие%'
	AND COALESCE(testing, FALSE)  = 'false'
	AND p.amount > 1
	AND to_date(p.moment) > '2021-08-26'
), 
paid_lessons AS (
-- list of all successfully finished and scheduled paid lessons 
    SELECT 
		lesson_id,
      	CASE
			WHEN lessons.grade_kind LIKE 'grade_1_4' THEN 'grade_1_4'
            WHEN lessons.grade_kind LIKE 'middle_school' THEN 'middle_school'
            WHEN lessons.grade_kind LIKE 'high_school' THEN 'high_school'
            ELSE 'undefined'
		END AS grade_kind,
		lessons.subject_id,
		lessons_students.student_id,
		created_at
    FROM raw_pg_services_distance_learning_public.lessons_students
	JOIN raw_pg_services_distance_learning_public.lessons ON lessons.id = lessons_students.lesson_id
    WHERE  1=1
    AND COALESCE(trial, FALSE) = 'false'
	AND status IN (0,1,2,3)
    ORDER BY created_at DESC 
),
first_ever_payments AS (
-- information about dates of the first successful payment for each user
    SELECT 
    	p.client_id,
        min(p.moment) as first_moment
    FROM raw_pg_services_distance_learning_public.payments AS p
	JOIN raw_pg_services_distance_learning_public.orders AS o on o.payment_id = p.id
	JOIN jsp ON jsp.payment_id = p.id
    WHERE  1=1
    AND o.state = 'paid'
	AND jsp.data['Description'] NOT LIKE 'Пробное занятие%'
	AND COALESCE(testing, FALSE) = 'false'
	AND p.amount > 1
    GROUP BY 1
), 
first_paid_lessons AS (
-- information about first finished paid lesson for each user
    SELECT
		lessons.grade_kind,
		lessons.subject_id,
		lessons_students.student_id,
		min(time_start) AS time_start
    FROM raw_pg_services_distance_learning_public.lessons_students
	JOIN raw_pg_services_distance_learning_public.lessons ON lessons.id = lessons_students.lesson_id
    WHERE 1=1
    AND COALESCE(trial, FALSE) = 'false'
    AND status IN (0,1,2,3)
    GROUP BY 1,2,3
    ORDER BY time_start DESC 
),
traffic_source AS (
-- getting information about users utm tags on the stage 'request created'. 
-- Based on this information we deterimine a traffic source for each user. 
	SELECT 
		traffic_source.parent_id,
		traffic_source.added_to_telemarketing_list_request,  
		students.id AS student_id,
        traffic_source.utm_medium,
        traffic_source.utm_source,
        traffic_source.utm_campaign,
        traffic_source.utm_content,
        traffic_source.utm_term,
        traffic_source.traffic_source,
        traffic_source.external_source_type
	FROM storage_distance_learning.traffic_source 
	LEFT JOIN raw_pg_services_distance_learning_public.students AS students USING (parent_id)
), 
traffic_source_lead AS (
-- getting information about users utm tags on the stage 'lead created'. 
-- Based on this information we deterimine a traffic source for each user.
	SELECT  
		traffic_source_lead.phone,
		students.id AS student_id,
        traffic_source_lead.created_at,
        traffic_source_lead.subject,
        traffic_source_lead.grade_kind,
        traffic_source_lead.utm_medium_lead,
        traffic_source_lead.utm_source_lead,
        traffic_source_lead.utm_campaign_lead,
        traffic_source_lead.utm_content_lead,
        traffic_source_lead.utm_term_lead,
        traffic_source_lead.traffic_source_lead,
        traffic_source_lead.external_source_type_lead
    FROM tmp_datamart.distance_learning_traffic_source_lead AS traffic_source_lead
	LEFT JOIN raw_pg_services_distance_learning_public.parents AS p ON p.phone = traffic_source_lead.phone
	LEFT JOIN raw_pg_services_distance_learning_public.students AS students ON students.parent_id = p.id
), 
superusers_lesson AS (
-- list of lessons that were created by admins instead of users
	SELECT 
		lcl.id AS lcl_id,
		student_id ,
		lesson_id ,
		time_start,
		date(l.created_at) as trial_created_at,
		subject_id,
		grade_kind
	FROM raw_pg_services_distance_learning_public.lesson_creation_logs AS lcl
	JOIN raw_pg_services_distance_learning_public.lessons AS l ON l.id = lcl.lesson_id
	WHERE l.trial = 'true'
), 
subject_name AS (
-- list of all school subjects that Uchi.Doma had 
	SELECT 
		DISTINCT 
			l.subject_id,
			CASE 
				WHEN l.subject_id = 1 THEN 'english'
				WHEN l.subject_id = 2 THEN 'mathematics'
				WHEN l.subject_id = 3 THEN 'programming'
				WHEN l.subject_id = 4 THEN 'russian'
				WHEN l.subject_id = 5 THEN 'socials'
				WHEN l.subject_id = 6 THEN 'history'
				WHEN l.subject_id = 7 THEN 'physics'
				WHEN l.subject_id = 8 THEN 'biology'
				WHEN l.subject_id = 9 THEN 'chemistry'
				WHEN l.subject_id = 10 THEN 'informatics'
				WHEN l.subject_id = 11 THEN 'nature'
				WHEN l.subject_id = 12 THEN 'computer_literacy'
				WHEN l.subject_id = 13 THEN 'ecology'
				WHEN l.subject_id = 14 THEN 'summer_program'
				ELSE 'undefined'
			END AS subject_name
	FROM raw_pg_services_distance_learning_public.lessons AS l
),
landing_amo_leads_info AS (
-- getting information about users phones and landing pages where they left their contact information in order to sign up for a trial lesson 
-- that's how we build up the first stage of the sales funneld - "lead created"
	SELECT 
		DISTINCT lal.phone,
		lal.subject,
		lal.page_url,
		lal.created_at as created_at,
		lal.tag,
		CASE
			WHEN substring(lal.page_url, 0,COALESCE(NULLIF(position('?' IN lal.page_url),0),LENGTH(lal.page_url))) LIKE '%%tilda%%' THEN 'tilda_tests'
			WHEN substring(lal.page_url, 0,COALESCE(NULLIF(position('?' IN lal.page_url),0),LENGTH(lal.page_url))) LIKE '%%gatsby%%' THEN 'gatsby_tests'
			WHEN substring(lal.page_url, 0,COALESCE(NULLIF(position('?' IN lal.page_url),0),LENGTH(lal.page_url))) LIKE '%%http://localhost:%%' THEN 'localhost_tests'
			WHEN substring(lal.page_url, 0,COALESCE(NULLIF(position('?' IN lal.page_url),0),LENGTH(lal.page_url))) LIKE '%%slot-select%%' THEN 'slot_select'
			WHEN substring(lal.page_url, 0,COALESCE(NULLIF(position('?' IN lal.page_url),0),LENGTH(lal.page_url))) LIKE '%%trial-lesson-sign-up%%' THEN 'trial-lesson-sign-up'
			ELSE 
				CASE
					WHEN RIGHT(substring(lal.page_url, 0,COALESCE(NULLIF(POSITION('?' IN lal.page_url),0),LENGTH(lal.page_url)+1)),1 )='/'
					THEN substring(lal.page_url, 0,COALESCE(NULLIF(POSITION('?' IN lal.page_url),0),LENGTH(lal.page_url)+1))
					ELSE concat(substring(lal.page_url, 0,COALESCE(NULLIF(POSITION('?' IN lal.page_url),0),LENGTH(lal.page_url)+1)-1) ,'/')
				END
		END AS page,
		ROW_NUMBER() OVER (PARTITION BY lal.phone, lal.subject,date(lal.created_at) ORDER BY lal.created_at) AS rn_op_landong
	FROM raw_pg_services_distance_learning_public.landing_amo_leads AS lal
	WHERE (lower(lal.name) NOT IN ('test', 'тест') OR lal.name IS NULL)  
), 
lal_sub AS (
-- creating special feature that allows us to make user segmentation in next CTEs
	SELECT 
		landing_amo_leads_info.*,
		CASE 
			WHEN (((landing_amo_leads_info.page LIKE '%%english2%%' OR landing_amo_leads_info.page LIKE '%%programming2%%') AND date(landing_amo_leads_info.created_at) >='2022-06-01') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/' OR landing_amo_leads_info.page LIKE '%%doma.uchi.ru/?%%') AND landing_amo_leads_info.created_at>'2022-06-13 17:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/tutors/' OR landing_amo_leads_info.page LIKE '%%doma.uchi.ru/math-school/') AND landing_amo_leads_info.created_at>'2022-06-15 10:25:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/middle-school/%%') AND landing_amo_leads_info.created_at>'2022-06-29 16:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/primary-school/') AND landing_amo_leads_info.created_at>'2022-07-22 10:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/english/' OR landing_amo_leads_info.page LIKE '%%doma.uchi.ru/programming/') AND landing_amo_leads_info.created_at>'2022-07-20 00:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR 
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/russian/') AND landing_amo_leads_info.created_at>'2022-08-01 00:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			OR
			(((landing_amo_leads_info.page LIKE '%%doma.uchi.ru/it-base/') AND landing_amo_leads_info.created_at>'2022-08-11 00:00:00') OR concat_ws('"' ,landing_amo_leads_info.tag) LIKE '%%funnel%%')
			THEN 1
			ELSE 0
		END AS op_landing
	FROM landing_amo_leads_info
),
lessons AS (
	SELECT  
		id,
		created_at,
		time_start,
   		student_id,
   		canceled_at,
   		subject_id,
   		trial,
   		kind,
   		ROW_NUMBER() OVER (PARTITION BY subject_id, student_id, l.time_start ORDER BY l.canceled_at NULLS LAST) as lesson_rn
	FROM raw_pg_services_distance_learning_public.lessons l
	JOIN raw_pg_services_distance_learning_public.lessons_students ls ON l.id=ls.lesson_id
	WHERE 1=1
	AND l.trial IS TRUE  
	AND l.kind NOT IN (2,5,6) 
),
subject_names AS (
-- getting names of all school subjects that Uchi.Doma online school has
	SELECT 
		id,
		lower(name) AS name
	FROM raw_pg_services_distance_learning_public.subjects
),
subject_names_adj AS (
-- making slight adjustments in subject names
	SELECT 
		subject_names.id,
		CASE 
			WHEN id = 1 THEN 'english'
			WHEN id = 2 THEN 'mathematics'
			WHEN id = 3 THEN 'programming'
			WHEN id = 4 THEN 'russian'
			WHEN id = 5 THEN 'socials'
			WHEN id = 6 THEN 'history'
			WHEN id = 7 THEN 'physics'
			WHEN id = 8 THEN 'biology'
			WHEN id = 9 THEN 'chemistry'
			WHEN id = 10 THEN 'informatics'
			WHEN id = 11 THEN 'nature'
			WHEN id = 12 THEN 'computer_literacy'
			WHEN id = 13 THEN 'ecology'
			WHEN id = 14 THEN 'summer_program'
			ELSE 'undefined'
		END AS name  
	FROM subject_names
),
lal AS (
-- final table that further will be used to get all information about the first sales funnel stage - "lead created"
	SELECT
		lal_sub.phone,
		lal_sub.subject,
		lower(subj.name) AS subject_name,
		lal_sub.created_at,
		l.id AS lesson_id,
		l.created_at AS trial_created_at,
		l.time_start AS trial_time_start,
		l.canceled_at,
		lal_sub.page,
		lal_sub.op_landing,
		lal_sub.page AS page_name,
		ROW_NUMBER() OVER (PARTITION BY lal_sub.phone, lal_sub.subject, lower(subj.name),lal_sub.created_at
						   ORDER BY greatest(
										unix_timestamp(l.created_at) - unix_timestamp(lal_sub.created_at),
										unix_timestamp(lal_sub.created_at)-unix_timestamp(l.created_at)
										)
							) AS rn_lead_created
	FROM lal_sub
	LEFT JOIN raw_pg_services_distance_learning_public.parents AS p ON p.phone = lal_sub.phone
	LEFT JOIN raw_pg_services_distance_learning_public.students AS s ON s.parent_id = p.id
	LEFT JOIN raw_pg_services_distance_learning_public.lessons_students AS ls ON ls.student_id = s.id
	LEFT JOIN lessons AS l 
		ON l.id = ls.lesson_id 
		AND lesson_rn=1
	LEFT JOIN subject_names_adj AS subj 
		ON subj.id = l.subject_id 
	WHERE 1=1
	AND l.trial IS TRUE 
	AND l.kind NOT IN (2,5,6) 
	-- this condtion lets to determine closest lead to created trial lesson
	AND lal_sub.created_at < l.created_at  
	-- looking for a closest lead within 2 month period (due to business processes)
	AND unix_timestamp(l.created_at) - unix_timestamp(lal_sub.created_at) < 5260000 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
), 
first_trial_sub AS (
	SELECT
		ls.student_id,
		l.id AS lesson_id,
		l.subject_id,
		l.grade_kind,
		l.created_at,
		ROW_NUMBER() OVER (PARTITION BY ls.student_id, l.subject_id ORDER BY l.canceled_at NULLS LAST) AS rn_first_trial
	FROM raw_pg_services_distance_learning_public.lessons AS l
	LEFT JOIN raw_pg_services_distance_learning_public.lessons_students AS ls ON ls.lesson_id = l.id
	WHERE l.trial IS TRUE
	AND l.kind NOT IN (2,5,6)
),
first_trial AS (
-- getting information about first trial lesson for each student (breakdown: student + subject)
	SELECT
		first_trial_sub.student_id,
		first_trial_sub.lesson_id,
		first_trial_sub.subject_id,
		first_trial_sub.grade_kind,
		first_trial_sub.created_at
	FROM first_trial_sub
	WHERE rn_first_trial = 1
),
first_trial_any_sub AS (
-- getting information about first trial lesson for each student (breakdown: just student without subject)
	SELECT
		ls.student_id,
		l.id AS lesson_id,
		l.subject_id,
		l.grade_kind,
		l.created_at,
		ROW_NUMBER() OVER (PARTITION BY ls.student_id ORDER BY l.canceled_at NULLS LAST) AS rn_first_trial_any
	FROM raw_pg_services_distance_learning_public.lessons AS l
	LEFT JOIN raw_pg_services_distance_learning_public.lessons_students AS ls ON ls.lesson_id = l.id
	WHERE l.trial IS TRUE
	AND l.kind NOT IN (2,5,6)
),
first_trial_any AS (
-- getting information about first trial lesson for each student (breakdown: just student)
	SELECT
		first_trial_any_sub.student_id,
		first_trial_any_sub.lesson_id,
		first_trial_any_sub.subject_id,
		first_trial_any_sub.grade_kind,
		first_trial_any_sub.created_at
	FROM first_trial_any_sub
	WHERE rn_first_trial_any = 1
),
op_landing AS (
	SELECT * FROM lal_sub
	WHERE rn_op_landong = 1
),
lal_wo_request_sub AS (
-- creating the first step of the sales funnel - 'lead created' for those cases when lead didn't convert to the next funnel stage
	SELECT
		s.id AS student_id,
		lal.phone,
		p.is_location_moscow,
		'' AS lesson_id,
		'' AS reason,
		lal.subject,
		CASE
			WHEN lal.subject = 'english' THEN 1
			WHEN lal.subject = 'mathematics' THEN 2
			WHEN lal.subject = 'programming' THEN 3
			WHEN lal.subject = 'russian' THEN 4
			WHEN lal.subject = 'socials' THEN 5
			WHEN lal.subject = 'history' THEN 6
			WHEN lal.subject = 'physics' THEN 7
			WHEN lal.subject = 'biology' THEN 8
			WHEN lal.subject = 'chemistry' THEN 9
			WHEN lal.subject = 'informatics' THEN 10
			WHEN lal.subject = 'nature' THEN 11
			WHEN lal.subject = 'computer_literacy' THEN 12
			WHEN lal.subject = 'ecology' THEN 13
			WHEN lal.subject = 'summer_program' THEN 14
			ELSE 0
		END subject_id,
		lal.grade_kind,
		CASE 
			WHEN (lal.subject = 'english' AND lal.grade_kind LIKE 'grade_1_4') THEN 'english_primary_school'
			WHEN (lal.subject = 'english' AND lal.grade_kind LIKE 'middle_school') THEN 'english_middle_school'
			WHEN (lal.subject = 'english' AND lal.grade_kind LIKE 'high_school') THEN 'english_exam'
			WHEN (lal.subject = 'mathematics' AND lal.grade_kind LIKE 'grade_1_4') THEN 'math_primary_school'
			WHEN (lal.subject = 'mathematics' AND lal.grade_kind LIKE 'middle_school') THEN 'math_middle_school'
			WHEN (lal.subject = 'mathematics' AND lal.grade_kind LIKE 'high_school') THEN 'math_exam'
			WHEN (lal.subject = 'programming' AND lal.grade_kind LIKE 'grade_1_4') THEN 'programming_primary_school'
			WHEN (lal.subject = 'programming' AND lal.grade_kind LIKE 'middle_school') THEN 'programming_middle_school'
			WHEN (lal.subject = 'russian' AND lal.grade_kind LIKE 'grade_1_4') THEN 'russian_primary_school'
			WHEN (lal.subject = 'russian' AND lal.grade_kind LIKE 'middle_school') THEN 'russian_middle_school'
			WHEN (lal.subject = 'russian' AND lal.grade_kind LIKE 'high_school') THEN 'russian_exam'
			WHEN (lal.subject = 'socials' AND lal.grade_kind LIKE 'middle_school') THEN 'socials_middle_school'
			WHEN (lal.subject = 'socials' AND lal.grade_kind LIKE 'high_school') THEN 'socials_exam'
			WHEN (lal.subject = 'history' AND lal.grade_kind LIKE 'middle_school') THEN 'history_middle_school'
			WHEN (lal.subject = 'history' AND lal.grade_kind LIKE 'high_school') THEN 'history_exam'
			WHEN (lal.subject = 'physics' AND lal.grade_kind LIKE 'middle_school') THEN 'physics_middle_school'
			WHEN (lal.subject = 'physics' AND lal.grade_kind LIKE 'high_school') THEN 'physics_exam'
			WHEN (lal.subject = 'biology' AND lal.grade_kind LIKE 'middle_school') THEN 'biology_middle_school'
			WHEN (lal.subject = 'biology' AND lal.grade_kind LIKE 'high_school') THEN 'biology_exam'
			WHEN (lal.subject = 'chemistry' AND lal.grade_kind LIKE 'middle_school') THEN 'chemistry_middle_school'
			WHEN (lal.subject = 'chemistry' AND lal.grade_kind LIKE 'high_school') THEN 'chemistry_exam'
			WHEN (lal.subject = 'informatics' AND lal.grade_kind LIKE 'high_school') THEN 'informatics_exam'
			WHEN (lal.subject = 'nature' AND lal.grade_kind LIKE 'grade_1_4') THEN 'nature'
			WHEN (lal.subject = 'computer_literacy' AND lal.grade_kind LIKE 'grade_1_4') THEN 'computer_literacy'
			WHEN (lal.subject = 'summer_program' AND lal.grade_kind LIKE 'grade_1_4') THEN 'summer_program'
			WHEN (lal.subject = 'ecology' AND lal.grade_kind LIKE 'grade_1_4') THEN 'ecology'
			ELSE 'undefined'
		END AS subject_name,
		'' AS sl_lesson_id,
		lal.created_at AS lal_created_at,
		'' AS first_trial_lesson,
		'' AS first_trial_any,
		op_landing.op_landing,
		'' AS lead_type,
		'' AS lead_type_2,
		lal.created_at AS lead_created,
		'' AS time_start,
		'' AS trial_created_at,
		'' AS trial_finished_at,
		'' AS subject_name_rev,
		'' AS first_purchase,
		CASE
			WHEN RIGHT(substring(lal.page_url, 0,COALESCE(NULLIF(POSITION('?' IN lal.page_url),0),LENGTH(lal.page_url)+1)),1 )='/'
			THEN substring(lal.page_url, 0,COALESCE(NULLIF(POSITION('?' IN lal.page_url),0),LENGTH(lal.page_url)+1))
			ELSE concat(substring(lal.page_url, 0,COALESCE(nullif(position('?' IN lal.page_url),0),LENGTH(lal.page_url)+1)-1) ,'/')
		END AS page_name,
		'' AS utm_medium_request,
		'' AS utm_source_request,
		'' AS utm_campaign_request,
		'' AS utm_term_request,
		'' AS utm_content_request,
		'' AS traffic_source_request,
		'' AS external_source_type_request,
		traffic_lead.utm_medium_lead,
		traffic_lead.utm_source_lead,
		traffic_lead.utm_campaign_lead,
		traffic_lead.utm_term_lead,
		traffic_lead.utm_content_lead,
		traffic_lead.traffic_source_lead,
		traffic_lead.external_source_type_lead,
		CASE 
			WHEN date_trunc('hour', p.created_at) = date_trunc('hour', lal.created_at) THEN 'new'
			ELSE 'current'
		END AS new_current_user_flag,
		'' AS payment_id, 
		'' AS amount,
		ROW_NUMBER() OVER (PARTITION BY lal.phone, lal.subject, lal.grade_kind, date_trunc('month',lal.created_at) 
							ORDER BY to_date(lal.created_at)) AS rn_lead_created
	FROM raw_pg_services_distance_learning_public.landing_amo_leads AS lal
	LEFT JOIN raw_pg_services_distance_learning_public.parents AS p ON p.phone = lal.phone
	LEFT JOIN raw_pg_services_distance_learning_public.students AS s ON s.parent_id = p.id
	/*LEFT JOIN lal AS lal_with 
		ON lal_with.phone = lal.phone
		AND lal_with.subject = lal.subject*/
	LEFT JOIN op_landing 
		ON op_landing.phone = lal.phone
		AND COALESCE(op_landing.subject, '0') = COALESCE(lal.subject, '0') 
		AND to_date(op_landing.created_at) = to_date(lal.created_at)
	LEFT JOIN tmp_datamart.distance_learning_traffic_source_lead AS traffic_lead 
		ON traffic_lead.phone = lal.phone
		AND traffic_lead.created_at = lal.created_at
	WHERE (lower(lal.name) NOT IN ('test', 'тест') OR lal.name IS NULL)  
	AND lal.created_at BETWEEN '2023-09-01' AND '2023-09-10'
),
lal_wo_request AS (
-- adding infromation about types of leads 
	SELECT
		DISTINCT lal_wo_request_sub.student_id,
		lal_wo_request_sub.phone,
		lal_wo_request_sub.is_location_moscow,
		lal_wo_request_sub.lesson_id,
		lal_wo_request_sub.reason,
		lal_wo_request_sub.subject_id,
		lal_wo_request_sub.grade_kind,
		lal_wo_request_sub.subject_name,
		lal_wo_request_sub.time_start,
		lal_wo_request_sub.sl_lesson_id,
		lal_wo_request_sub.lal_created_at,
		lal_wo_request_sub.first_trial_lesson,
		lal_wo_request_sub.first_trial_any,
		lal_wo_request_sub.op_landing,
		'by_operator' AS lead_type,
		CASE
			WHEN first_trial.created_at IS NOT NULL AND first_trial.created_at < lal_wo_request_sub.lal_created_at THEN 'by_operator: rescheduled_lead' 
			WHEN first_trial_any.created_at IS NOT NULL AND (lal_wo_request_sub.subject_name IS NULL OR lal_wo_request_sub.subject_name ='undefined') AND first_trial_any.created_at < lal_wo_request_sub.lal_created_at THEN 'by_operator: rescheduled_lead'  
			WHEN lal_wo_request_sub.op_landing = 1 AND (first_trial.created_at IS NULL OR first_trial.created_at = lal_wo_request_sub.lal_created_at) THEN 'by_operator: funnel_page_new_lead' 
			WHEN lal_wo_request_sub.op_landing = 1 AND (first_trial_any.created_at IS NULL OR first_trial_any.created_at = lal_wo_request_sub.lal_created_at) THEN 'by_operator: funnel_page_new_lead' 
			WHEN lal_wo_request_sub.op_landing = 0 AND (first_trial.created_at IS NULL OR first_trial.created_at = lal_wo_request_sub.lal_created_at) THEN 'by_operator: not_funnel_page_new_lead' 
			WHEN lal_wo_request_sub.op_landing = 0 AND (first_trial_any.created_at IS NULL OR first_trial_any.created_at = lal_wo_request_sub.lal_created_at) THEN 'by_operator: not_funnel_page_new_lead' 
		END AS lead_type_2,
		lal_wo_request_sub.lead_created,
		lal_wo_request_sub.trial_created_at,
		lal_wo_request_sub.trial_finished_at,
		lal_wo_request_sub.subject_name_rev,
		lal_wo_request_sub.first_purchase,
		lal_wo_request_sub.page_name,
		lal_wo_request_sub.utm_medium_request,
		lal_wo_request_sub.utm_source_request,
		lal_wo_request_sub.utm_campaign_request,
		lal_wo_request_sub.utm_term_request,
		lal_wo_request_sub.utm_content_request,
		lal_wo_request_sub.traffic_source_request,
		lal_wo_request_sub.external_source_type_request,
		lal_wo_request_sub.utm_medium_lead,
		lal_wo_request_sub.utm_source_lead,
		lal_wo_request_sub.utm_campaign_lead,
		lal_wo_request_sub.utm_term_lead,
		lal_wo_request_sub.utm_content_lead,
		lal_wo_request_sub.traffic_source_lead,
		lal_wo_request_sub.external_source_type_lead,
		lal_wo_request_sub.new_current_user_flag,
		lal_wo_request_sub.payment_id,
		lal_wo_request_sub.amount
	FROM lal_wo_request_sub
	LEFT JOIN first_trial 
		ON first_trial.student_id = lal_wo_request_sub.student_id
		AND first_trial.subject_id = lal_wo_request_sub.subject_id
		AND first_trial.created_at <= lal_wo_request_sub.lal_created_at  
	LEFT JOIN first_trial_any 
		ON first_trial_any.student_id = lal_wo_request_sub.student_id
		AND first_trial_any.created_at <= lal_wo_request_sub.lal_created_at  
	WHERE rn_lead_created = 1
),
trial_created_sub AS (
-- crerating second step in a sales funnel - 'trial lesson created'
	SELECT 
		s.id AS student_id,
   		s.first_name,
       	s.last_name,
       	p.is_location_moscow,
        l.id AS lesson_id,
        lss.reason,
        l.subject_id,
        CASE
           WHEN l.grade_kind LIKE 'grade_1_4' THEN 'grade_1_4'
           WHEN l.grade_kind LIKE 'middle_school' THEN 'middle_school'
           WHEN l.grade_kind LIKE 'high_school' THEN 'high_school'
           ELSE 'undefined'
        END AS grade_kind,
        l.created_at,
        l.time_start,
        ROW_NUMBER() OVER (PARTITION BY s.id, l.subject_id, l.grade_kind, l.time_start ORDER BY l.canceled_at NULLS LAST) AS trial_created_rn
	FROM raw_pg_services_distance_learning_public.lessons_students AS ls
	JOIN raw_pg_services_distance_learning_public.students AS s ON s.id = ls.student_id -- LEFT JOIN - JOIN
	JOIN raw_pg_services_distance_learning_public.parents AS p ON p.id = s.parent_id -- LEFT JOIN - JOIN
	JOIN raw_pg_services_distance_learning_public.lessons AS l ON l.id = ls.lesson_id -- LEFT JOIN - JOIN
	LEFT JOIN raw_pg_services_distance_learning_public.lessons_students_statuses AS lss ON lss.lesson_id = l.id
	WHERE 1=1
	AND l.trial IS TRUE 
 	AND l.kind NOT IN (2,5,6)
 	AND (lower(s.first_name) NOT LIKE ('(test|тест|донна|методист)')
		OR lower(s.last_name) NOT LIKE ('(test|тест|донна|методист)'))
  	AND (lower(p.first_name) NOT LIKE ('(test|тест|донна|методист)')
     	 OR lower(p.last_name) NOT LIKE ('(test|тест|донна|методист)'))
	AND (p.phone IS NOT NULL OR s.phone IS NOT NULL)
	AND l.created_at BETWEEN '2023-09-01' AND '2023-09-10'
),
trial_created AS (
-- final table for the second stage of the sales funnel - 'trial lesson created'
	SELECT 
		trial_created_sub.*,
        CASE
			WHEN trial_created_sub.subject_id = 1 THEN 'english'
            WHEN trial_created_sub.subject_id = 2 THEN 'mathematics'
			WHEN trial_created_sub.subject_id = 3 THEN 'programming'
			WHEN trial_created_sub.subject_id = 4 THEN 'russian'
			WHEN trial_created_sub.subject_id = 5 THEN 'socials'
			WHEN trial_created_sub.subject_id = 6 THEN 'history'
			WHEN trial_created_sub.subject_id = 7 THEN 'physics'
			WHEN trial_created_sub.subject_id = 8 THEN 'biology'
			WHEN trial_created_sub.subject_id = 9 THEN 'chemistry'
			WHEN trial_created_sub.subject_id = 10 THEN 'informatics'
			WHEN trial_created_sub.subject_id = 11 THEN 'nature'
			WHEN trial_created_sub.subject_id = 12 THEN 'computer_literacy'
			WHEN trial_created_sub.subject_id = 13 THEN 'ecology'
			WHEN trial_created_sub.subject_id = 14 THEN 'summer_program'
		ELSE 'undefined'
		END AS subject_name_wo_grade
	FROM trial_created_sub
	WHERE trial_created_sub.trial_created_rn = 1
),
finished_trial AS (
-- creating third step of the sales funnel - 'trial lesson finished'
	SELECT
		ls.student_id,
		CASE
			WHEN l.grade_kind LIKE 'grade_1_4' THEN 'grade_1_4'
			WHEN l.grade_kind LIKE 'middle_school' THEN 'middle_school'
			WHEN l.grade_kind LIKE 'high_school' THEN 'high_school'
			ELSE 'undefined'
		END AS grade_kind,
		l.id AS lesson_id,
		l.subject_id,
		l.time_start
	FROM raw_pg_services_distance_learning_public.lessons_students AS ls
	JOIN raw_pg_services_distance_learning_public.students AS s ON s.id = ls.student_id -- LEFT JOIN - JOIN
	JOIN raw_pg_services_distance_learning_public.parents AS p ON p.id = s.parent_id -- LEFT JOIN - JOIN
	JOIN raw_pg_services_distance_learning_public.lessons AS l ON l.id = ls.lesson_id -- LEFT JOIN - JOIN
	WHERE 1=1
	AND l.trial IS TRUE
	AND (l.test IS NULL OR l.test = 'f')
	AND l.status IN (2,3)
),
first_purch_sub1 AS (
-- creating fourth step of the sales funnel - 'first purchase'
-- payments that were made before '2021-08-27': the moment when 'union balance' feature was released
-- we need to calculate revenue in different ways before and after this feature release
	SELECT
		p.client_id,
		p.id AS payment_id,
		to_date(p.moment) AS moment,
		p.amount,
		p.status,
		CASE
			WHEN jsp.data['Description'] LIKE '%%англ%%' THEN 1
			WHEN jsp.data['Description'] LIKE '%%матем%%' THEN 2
		END AS subject
	FROM raw_pg_services_distance_learning_public.payments AS p
	JOIN jsp ON jsp.client_id = p.client_id AND jsp.payment_id = p.id -- LEFT JOIN - JOIN
	WHERE 1=1
	AND COALESCE(p.testing, FALSE) = FALSE
	AND p.amount > 1
	AND to_date(p.moment) < '2021-08-27'
),
first_purch_sub2 AS (
	SELECT
		first_purch_sub1.*,
		ROW_NUMBER() OVER (PARTITION BY first_purch_sub1.client_id, first_purch_sub1.subject ORDER BY first_purch_sub1.moment) AS first_purch_rn
	FROM first_purch_sub1
),
first_purchase_before AS (
	SELECT
		first_purch_sub2.client_id,
		first_purch_sub2.amount,
		first_purch_sub2.subject,
		first_purch_sub2.moment
	FROM first_purch_sub2
	WHERE 1=1
	AND (first_purch_rn = 1 OR first_purch_rn IS NULL)
),
first_purchase_after AS (
-- payments that were made starting from '2021-08-27': the moment when 'union balance' feature was released
	SELECT 
		np.client_id,
		np.id AS payment_id,
		np.amount,
		np.moment,
		fpl.subject_id,
		CASE
			WHEN (fpl.subject_id = 1 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'english_primary_school'
			WHEN (fpl.subject_id = 1 AND fpl.grade_kind LIKE 'middle_school') THEN 'english_middle_school'
			WHEN (fpl.subject_id = 1 AND fpl.grade_kind LIKE 'high_school') THEN 'english_exam'
			WHEN (fpl.subject_id = 2 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'math_primary_school'
			WHEN (fpl.subject_id = 2 AND fpl.grade_kind LIKE 'middle_school') THEN 'math_middle_school'
			WHEN (fpl.subject_id = 2 AND fpl.grade_kind LIKE 'high_school') THEN 'math_exam'
			WHEN (fpl.subject_id = 3 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'programming_primary_school'
			WHEN (fpl.subject_id = 3 AND fpl.grade_kind LIKE 'middle_school') THEN 'programming_middle_school'
			WHEN (fpl.subject_id = 4 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'russian_primary_school'
			WHEN (fpl.subject_id = 4 AND fpl.grade_kind LIKE 'middle_school') THEN 'russian_middle_school'
			WHEN (fpl.subject_id = 4 AND fpl.grade_kind LIKE 'high_school') THEN 'russian_exam'
			WHEN (fpl.subject_id = 5 AND fpl.grade_kind LIKE 'middle_school') THEN 'socials_middle_school'
			WHEN (fpl.subject_id = 5 AND fpl.grade_kind LIKE 'high_school') THEN 'socials_exam'
			WHEN (fpl.subject_id = 6 AND fpl.grade_kind LIKE 'middle_school') THEN 'history_middle_school'
			WHEN (fpl.subject_id = 6 AND fpl.grade_kind LIKE 'high_school') THEN 'history_exam'
			WHEN (fpl.subject_id = 7 AND fpl.grade_kind LIKE 'middle_school') THEN 'physics_middle_school'
			WHEN (fpl.subject_id = 7 AND fpl.grade_kind LIKE 'high_school') THEN 'physics_exam'
			WHEN (fpl.subject_id = 8 AND fpl.grade_kind LIKE 'middle_school') THEN 'biology_middle_school'
			WHEN (fpl.subject_id = 8 AND fpl.grade_kind LIKE 'high_school') THEN 'biology_exam'
			WHEN (fpl.subject_id = 9 AND fpl.grade_kind LIKE 'middle_school') THEN 'chemistry_middle_school'
			WHEN (fpl.subject_id = 9 AND fpl.grade_kind LIKE 'high_school') THEN 'chemistry_exam'
			WHEN (fpl.subject_id = 10 AND fpl.grade_kind LIKE 'high_school') THEN 'informatics_exam'
			WHEN (fpl.subject_id = 11 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'nature'
			WHEN (fpl.subject_id = 12 AND fpl.grade_kind LIKE 'grade_1_4') THEN 'computer_literacy'
			ELSE 'undefined'
		END AS subject_name_rev,
		CASE
			WHEN fp.first_moment = np.moment THEN np.moment
			WHEN fpl.student_id IS NOT NULL THEN np.moment
			ELSE NULL
		END AS segment
		FROM new_payments AS np
		LEFT JOIN first_ever_payments AS fp ON fp.client_id = np.client_id
		LEFT JOIN first_paid_lessons AS fpl ON fpl.student_id = np.client_id
                              				AND fpl.time_start BETWEEN np.moment AND np.moment + INTERVAL '1 month' + INTERVAL '30 days'
                              				AND fpl.time_start BETWEEN np.moment AND COALESCE(next_payment_moment, '2999-12-31')
       	WHERE 1=1
		AND to_date(fp.first_moment) = np.moment OR fpl.student_id IS NOT NULL
       	GROUP BY 1,2,3,4,5,6,7
),
sub AS (
-- put together funnel stages: "trial lesson created - trial lesson finished - first purchase" 
-- keep in mind that it should be one extra step before - "lead created"
	SELECT
		 trial_created.student_id,
		 trial_created.is_location_moscow,
         trial_created.lesson_id,
         trial_created.reason,
         trial_created.subject_id,
         trial_created.grade_kind,
         trial_created.subject_name_wo_grade,
         trial_created.created_at as trial_created_at,
         trial_created.created_at as trial_created_at_full,
         trial_created.time_start,
         to_date(finished_trial.time_start) AS trial_finished_at,
         to_date(first_purchase_before.moment) AS first_purchase_before,
         first_purchase_after.subject_name_rev,
         to_date(first_purchase_after.moment) AS first_purchase_after,
         first_purchase_after.payment_id,
         first_purchase_after.amount
	FROM trial_created 
	LEFT JOIN finished_trial 
		ON finished_trial.student_id = trial_created.student_id
		AND finished_trial.subject_id = trial_created.subject_id
		AND finished_trial.grade_kind = trial_created.grade_kind
		AND finished_trial.time_start = trial_created.time_start  
	LEFT JOIN first_purchase_before
		ON first_purchase_before.client_id = trial_created.student_id
		AND first_purchase_before.subject = trial_created.subject_id
	LEFT JOIN first_purchase_after
		ON first_purchase_after.client_id = trial_created.student_id
		AND COALESCE(first_purchase_after.subject_id, trial_created.subject_id) = trial_created.subject_id
	WHERE lower(trial_created.first_name) NOT IN ('test', 'тест', 'донна', 'методист')
),
final_sub AS (
-- adding all necessary information from previous CTEs to make possible further creation of needed breakdowns and filters
	SELECT
		DISTINCT rn_lead_created, 
		sub.student_id,
		COALESCE(p.phone, s.phone) AS phone, 
		sub.is_location_moscow,
        sub.lesson_id,
        sub.reason,
        sub.subject_id,
        sub.grade_kind,
   		CASE
            WHEN (sub.subject_id = 1 AND sub.grade_kind LIKE 'grade_1_4') THEN 'english_primary_school'
            WHEN (sub.subject_id = 1 AND sub.grade_kind LIKE 'middle_school') THEN 'english_middle_school'
            WHEN (sub.subject_id = 1 AND sub.grade_kind LIKE 'high_school') THEN 'english_exam'
            WHEN (sub.subject_id = 2 AND sub.grade_kind LIKE 'grade_1_4') THEN 'math_primary_school'
            WHEN (sub.subject_id = 2 AND sub.grade_kind LIKE 'middle_school')THEN 'math_middle_school'
            WHEN (sub.subject_id = 2 AND sub.grade_kind LIKE 'high_school') THEN 'math_exam'
            WHEN (sub.subject_id = 3 AND sub.grade_kind LIKE 'grade_1_4') THEN 'programming_primary_school'
            WHEN (sub.subject_id = 3 AND sub.grade_kind LIKE 'middle_school') THEN 'programming_middle_school'
            WHEN (sub.subject_id = 4 AND sub.grade_kind LIKE 'grade_1_4') THEN 'russian_primary_school'
            WHEN (sub.subject_id = 4 AND sub.grade_kind LIKE 'middle_school') THEN 'russian_middle_school'
            WHEN (sub.subject_id = 4 AND sub.grade_kind LIKE 'high_school') THEN 'russian_exam'
            WHEN (sub.subject_id = 5 AND sub.grade_kind LIKE 'middle_school')THEN 'socials_middle_school'
            WHEN (sub.subject_id = 5 AND sub.grade_kind LIKE 'high_school') THEN 'socials_exam'
            WHEN (sub.subject_id = 6 AND sub.grade_kind LIKE 'middle_school')THEN 'history_middle_school'
            WHEN (sub.subject_id = 6 AND sub.grade_kind LIKE 'high_school') THEN 'history_exam'
            WHEN (sub.subject_id = 7 AND sub.grade_kind LIKE 'middle_school')THEN 'physics_middle_school'
            WHEN (sub.subject_id = 7 AND sub.grade_kind LIKE 'high_school') THEN 'physics_exam'
            WHEN (sub.subject_id = 8 AND sub.grade_kind LIKE 'middle_school')THEN 'biology_middle_school'
            WHEN (sub.subject_id = 8 AND sub.grade_kind LIKE 'high_school') THEN 'biology_exam'
            WHEN (sub.subject_id = 9 AND sub.grade_kind LIKE 'middle_school')THEN 'chemistry_middle_school'
            WHEN (sub.subject_id = 9 AND sub.grade_kind LIKE 'high_school') THEN 'chemistry_exam'
            WHEN (sub.subject_id = 10 AND sub.grade_kind LIKE 'high_school') THEN 'informatics_exam'
            WHEN (sub.subject_id = 11 AND sub.grade_kind LIKE 'grade_1_4') THEN 'nature'
            WHEN (sub.subject_id = 12 AND sub.grade_kind LIKE 'grade_1_4') THEN 'computer_literacy'
            ELSE 'undefined'
		END AS subject_name,
        sub.time_start,
        sub.trial_created_at AS trial_created_at,
        sub.trial_finished_at AS trial_finished_at,
        sub.subject_name_rev,
        COALESCE(sub.first_purchASe_before, sub.first_purchASe_after) AS first_purchASe,
        sl.lesson_id AS sl_lesson_id,
        lal.created_at AS lal_created_at,
        first_trial.created_at AS first_trial_lesson,
        first_trial_any.created_at AS first_trial_any,
        lal.subject,
        lal.subject_name AS subject_name_extra,
        lal.op_landing,
        CASE 
			WHEN sl.lesson_id IS NULL THEN sub.trial_created_at - INTERVAL '1 minute'
			WHEN sl.lesson_id IS NOT NULL AND lal.created_at IS NULL THEN sub.trial_created_at
			WHEN sl.lesson_id IS NOT NULL AND lal.created_at IS NOT NULL THEN lal.created_at
		END AS lead_created,
		CASE 
			WHEN sl.lesson_id IS NULL THEN 'by_user'
			ELSE 'by_operator'
		END AS lead_type,
		CASE 
			WHEN sl.lesson_id IS NULL AND first_trial_any.created_at = sub.trial_created_at_full THEN 'by_user: new'
			WHEN sl.lesson_id IS NULL AND first_trial_any.created_at != sub.trial_created_at_full THEN 'by_user: current'
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NOT NULL
				AND first_trial.created_at IS NOT NULL
				AND first_trial.created_at < lal.created_at IS FALSE
				AND lal.op_lANDing = 1
			THEN 'by_operator: funnel_page_new_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NULL
				AND first_trial_any.created_at < lal.created_at IS FALSE
				AND lal.op_lANDing = 1
			THEN 'by_operator: funnel_page_new_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NOT NULL
				AND first_trial.created_at IS NOT NULL
				AND first_trial.created_at < lal.created_at IS FALSE
				AND lal.op_lANDing = 0
			THEN 'by_operator: not_funnel_page_new_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NULL
				AND first_trial_any.created_at < lal.created_at IS FALSE
				AND lal.op_lANDing = 0
			THEN 'by_operator: not_funnel_page_new_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NOT NULL
				AND first_trial.created_at < lal.created_at IS TRUE
			THEN 'by_operator: rescheduled_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NOT NULL
				AND COALESCE(lal.subject, lal.subject_name) IS NULL
				AND first_trial_any.created_at < lal.created_at IS TRUE
			THEN 'by_operator: rescheduled_lead' 
			WHEN sl.lesson_id IS NOT NULL
				AND lal.created_at IS NULL
			THEN 'by_operator: other' 
			ELSE 'undefined'
		END AS lead_type_2,
		lal.page_name,
		traffic_source.utm_medium,
		traffic_source.utm_source,
		traffic_source.utm_campaign,
		traffic_source.utm_term,
		traffic_source.utm_content,
		traffic_source.traffic_source,
		traffic_source.external_source_type,
		CASE 
			WHEN date_trunc('hour', p.created_at) = date_trunc('hour', lal.created_at) THEN 'new'
			ELSE 'current'
		END AS new_current_user_flag,
		sub.payment_id,
        sub.amount
	FROM sub 
	JOIN raw_pg_services_distance_learning_public.lessons_students AS ls ON ls.student_id = sub.student_id
	JOIN raw_pg_services_distance_learning_public.lessons AS l 
		ON l.id = sub.lesson_id
		AND l.subject_id = sub.subject_id
		AND sub.trial_created_at = l.created_at
	LEFT JOIN superusers_lesson AS sl 
		ON sl.student_id = sub.student_id
		AND sl.subject_id = sub.subject_id
		AND sl.lesson_id = sub.lesson_id
	LEFT JOIN raw_pg_services_distance_learning_public.students as s on s.id = ls.student_id
	JOIN raw_pg_services_distance_learning_public.parents as p on p.id = s.parent_id  
	JOIN subject_name as sn on sn.subject_id = l.subject_id 
	LEFT JOIN lal 
		ON lal.phone = p.phone
		AND (CASE
				WHEN sub.lesson_id != 0 THEN sub.subject_name_wo_grade = sn.subject_name
				WHEN sub.lesson_id = 0 THEN lal.subject = sn.subject_name
			END)
		AND (lal.created_at + INTERVAL '2 weeks' > l.created_at)
		AND lal.created_at < l.created_at
		AND rn_lead_created = 1
		AND lal.lesson_id = l.id
	LEFT JOIN first_trial 
		ON first_trial.student_id = sub.student_id
		AND first_trial.subject_id = sub.subject_id
	LEFT JOIN first_trial_any ON first_trial_any.student_id = sub.student_id
	LEFT JOIN traffic_source 
		ON traffic_source.student_id = sub.student_id
		AND (sub.trial_created_at <= (traffic_source.added_to_telemarketing_list_request + INTERVAL '54 days') 
			OR traffic_source.added_to_telemarketing_list_request IS NULL) 
),
final AS (
-- adding information about traffic sources on the funnel stage 'lead created'
-- thus we have information about traffic source both on the stage 'lead created' and on the stage 'trial created'
	SELECT 
		DISTINCT final_sub.student_id,
		final_sub.phone,
		final_sub.is_location_moscow,
		final_sub.lesson_id,
		final_sub.reason,
		final_sub.subject_id,
		final_sub.grade_kind,
		final_sub.subject_name,
		final_sub.time_start,
		final_sub.sl_lesson_id,
		final_sub.lal_created_at,
		final_sub.first_trial_lesson,
		final_sub.first_trial_any,
		final_sub.op_landing,
		final_sub.lead_type,
		final_sub.lead_type_2,
		final_sub.lead_created,
		to_date(final_sub.trial_created_at) AS trial_created_at,
		final_sub.trial_finished_at,
		final_sub.subject_name_rev,
		final_sub.first_purchase,
		final_sub.page_name,
		final_sub.utm_medium AS utm_medium_request,
		final_sub.utm_source AS utm_source_request,
		final_sub.utm_campaign AS utm_campaign_request,
		final_sub.utm_term AS utm_term_request,
		final_sub.utm_content AS utm_content_request,
		final_sub.traffic_source AS traffic_source_request,
		final_sub.external_source_type AS external_source_type_request,
		CASE
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.utm_medium
			ELSE traffic_source_lead.utm_medium_lead
		END AS utm_medium_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.utm_source
			ELSE traffic_source_lead.utm_source_lead
		END AS utm_source_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.utm_campaign
			ELSE traffic_source_lead.utm_campaign_lead
		END AS utm_campaign_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.utm_term
			ELSE traffic_source_lead.utm_term_lead
		END AS utm_term_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.utm_content
			ELSE traffic_source_lead.utm_content_lead
		END AS utm_content_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.traffic_source
			ELSE traffic_source_lead.traffic_source_lead
		END AS traffic_source_lead,
		CASE 
			WHEN (traffic_source_lead.utm_medium_lead IS NULL 
					AND traffic_source_lead.utm_source_lead IS NULL
					AND traffic_source_lead.utm_campaign_lead IS NULL 
					AND traffic_source_lead.utm_content_lead IS NULL 
					AND traffic_source_lead.utm_term_lead IS NULL 
					AND traffic_source_lead.traffic_source_lead IS NULL 
					AND traffic_source_lead.external_source_type_lead IS NULL)
			THEN final_sub.external_source_type
			ELSE traffic_source_lead.external_source_type_lead
		END AS external_source_type_lead,
		final_sub.new_current_user_flag,
		final_sub.payment_id,
		round(final_sub.amount, 0) AS amount
	FROM final_sub 
	LEFT JOIN traffic_source_lead 
		ON traffic_source_lead.phone = final_sub.phone
		AND traffic_source_lead.created_at = final_sub.lead_created
),
all_together AS (
-- adding those leads that were not converted to the next funnel stages
	select
		final.*,1 AS union_rn
	FROM final 
	UNION ALL 
	SELECT 
		*,2  
	FROM lal_wo_request AS lwr
),
full_table_sub AS (
-- adding flags that further will allow us to create breakdowns by periods: day, week, month (this will be done on Tableau level)
	SELECT
		*,
		CASE
			WHEN trial_created_at < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date(lead_created) ORDER BY union_rn, lead_created)
			WHEN trial_created_at > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, trial_created_at, subject_name ORDER BY union_rn, trial_created_at,lesson_id)
		END AS rn_daily,
		CASE
			WHEN lead_created < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date(lead_created) ORDER BY union_rn, lead_created)
			WHEN lead_created > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, lead_created, subject_name ORDER BY union_rn, lead_created,lesson_id)
		END AS rn_daily_lead,
		CASE
			WHEN trial_created_at < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('week', lead_created) ORDER BY union_rn, lead_created)
			WHEN trial_created_at > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('week', trial_created_at), subject_name ORDER BY union_rn, trial_created_at,lesson_id)
		END AS rn_weekly,
		CASE
			WHEN lead_created < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('week', lead_created) ORDER BY union_rn, lead_created)
			WHEN lead_created > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('week', lead_created), subject_name ORDER BY union_rn, lead_created,lesson_id)
		END AS rn_weekly_lead,
		CASE
			WHEN trial_created_at < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('month', lead_created) ORDER BY union_rn, lead_created)
			WHEN trial_created_at > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('month', trial_created_at), subject_name ORDER BY union_rn, trial_created_at,lesson_id)
		END AS rn_monthly,
		CASE
			WHEN lead_created < '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('month', lead_created) ORDER BY union_rn, lead_created)
			WHEN lead_created > '2018-01-01' THEN ROW_NUMBER () OVER (PARTITION BY phone, date_trunc('month', lead_created), subject_name ORDER BY union_rn, lead_created,lesson_id)
		END AS rn_monthly_lead
	FROM all_together
),
full_table AS (
-- leaving just needed lines from the whole table according to business logic
	SELECT 
		*
	FROM full_table_sub  
	WHERE 1=1
	AND (full_table_sub.union_rn = 1 or full_table_sub.rn_daily = 1)
	ORDER BY trial_created_at, lead_created	
),
month_type AS (
	SELECT 
		DISTINCT student_id,
		subject_name,
		phone,
		lead_type,
		lead_type_2,
		CASE 
			WHEN trial_created_at > '2018-01-01' THEN date_trunc('month',trial_created_at)
			ELSE date_trunc('month',lead_created) 
		END AS lead_request_month
	FROM full_table
	WHERE rn_monthly = 1
),
week_type AS (
	SELECT 
		DISTINCT student_id,
		subject_name,
		phone,
		lead_type,
		lead_type_2,
		CASE 
			WHEN trial_created_at > '2018-01-01' THEN date_trunc('week',trial_created_at)
			ELSE date_trunc('week',lead_created) 
		END AS lead_request_week
	FROM full_table
	WHERE rn_weekly=1
),
day_type AS (
	SELECT 
		DISTINCT student_id,
		subject_name,
		phone,
		lead_type,
		lead_type_2,
		CASE 
			WHEN trial_created_at > '2018-01-01' THEN date(trial_created_at)
			ELSE date(lead_created) 
		END AS lead_request_day
	FROM full_table
	WHERE rn_daily=1
)
SELECT  
	DISTINCT ft.*,
	COALESCE(mt.lead_type, ft.lead_type) AS  lead_type_month,
	COALESCE(mt.lead_type_2, ft.lead_type_2) AS  lead_type_2_month,
	COALESCE(wt.lead_type, ft.lead_type) AS  lead_type_week,
	COALESCE(wt.lead_type_2, ft.lead_type_2) AS  lead_type_2_week,
	COALESCE(dt.lead_type, ft.lead_type) AS  lead_type_day,
	COALESCE(dt.lead_type_2, ft.lead_type_2) AS  lead_type_2_day
FROM full_table AS ft
LEFT JOIN month_type AS mt ON COALESCE(mt.phone, mt.student_id) = COALESCE(ft.phone,ft.student_id) 
							AND (CASE 
									WHEN ft.subject_name='undefined' THEN 1=1 
									ELSE COALESCE(mt.subject_name, '0') = COALESCE(ft.subject_name, '0')
								END)
							AND lead_request_month = 
								(CASE 
									WHEN trial_created_at>'2018-01-01' THEN date_trunc('month',trial_created_at) 
									ELSE date_trunc('month',lead_created)
								END)
LEFT JOIN week_type AS wt ON COALESCE(wt.student_id, wt.phONe) = COALESCE(ft.student_id, ft.phONe) 
							AND (CASE 
									WHEN ft.subject_name='undefined' THEN 1=1 
									ELSE COALESCE(wt.subject_name, '0') = COALESCE(ft.subject_name, '0')
								END)
							AND lead_request_week = 
								(CASE 
									WHEN trial_created_at>'2018-01-01' THEN date_trunc('week',trial_created_at) 
									ELSE date_trunc('week',lead_created)
								END)
LEFT JOIN day_type AS dt ON COALESCE(dt.student_id, dt.phONe) = COALESCE(ft.student_id, ft.phONe) 
							AND (CASE 
									WHEN ft.subject_name='undefined' THEN 1=1 
									ELSE COALESCE(dt.subject_name, '0') = COALESCE(ft.subject_name, '0')
								END)
							AND lead_request_day = 
								(CASE 
									WHEN trial_created_at>'2018-01-01' THEN date_trunc('day', trial_created_at) 
									ELSE date_trunc('day', lead_created)
								END)
ORDER BY ft.lead_created 
