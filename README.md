# Biz-Ops-Table


SELECT bop.*,
	hrcmb.sf_lpn_nudge_hours,
	hrcmb.sf_lpn_review_hours,
	hrcmb.sf_rn_review_hours,
	hrcmb.sf_md_review_hours,
	hrcmb.sf_p2p_review_hours,
	betos.rbcs_id,
	betos.rbcs_cat_desc as betos_category_description,
	betos.rbcs_cat_subcat as betos_sub_category,
	betos.rbcs_family_desc as betos_family,
	soowp.tat_due_date,
	soowp.missing_info_attempts,
	srp.issingleserviceauth,
	srp.id,
	srp.primarydiagnosis_code,
	srp.primarydiagnosis_description,
	srp.cardio_procedurecode_flag,
	srp.patient_coverage_financialproductcode,
	srp.patient_coverage_grouperid,
	srp.patient_coverage_groupid,
	srp.patient_coverage_lineofbusinessdescription,
	srp.patient_coverage_lineofbusiness,
	srp.patient_coverage_plantype,
	srp.patient_coverage_productid,
	srp.patient_coverage_platformcode,
	srp.patient_coverage_policytype,
	srp.advancedimagingfindings_confidencescore,
	srp.advancedimagingfindings_numberofattachmentsanalyzed,
	srp.expedited,
	srp.encountertype,
	rt.title as auth_rule_triggered,
	rt.rule_link as auth_rule_triggered_link,
	rt.rule_value as auth_rule_triggered_set_value,
	rt.rule_ran_sequence as auth_rule_triggered_rule_run_sequence,
	CASE
		WHEN aa_rule.ruleid IS NULL THEN 'Not Enough Px & LOB Volume' ELSE aa_rule.ruleid
	END as common_aarule_id,
	CASE
		WHEN aa_rule.title IS NULL THEN 'Not Enough Px & LOB Volume' ELSE aa_rule.title
	END as common_aarule_title,
	CASE
		WHEN aa_rule.rule_link IS NULL THEN 'Not Enough Px & LOB Volume' ELSE aa_rule.rule_link
	END as common_aarule_link,
	COALESCE(aa_rule.auto_approved_auth_by_lobpx, 0) as common_aarule_auth_count,
	CASE
		WHEN meetings_and_training_time > 1
		AND total_auth_count > 1 THEN (meetings_and_training_time / total_auth_count) ELSE 0
	END as meetings_and_training_time,
	CASE
		WHEN non_production_time > 1
		AND total_auth_count > 1 THEN (non_production_time / total_auth_count) ELSE 0
	END as non_production_time,
	CASE
		WHEN other_aux_time > 1
		AND total_auth_count > 1 THEN (other_aux_time / total_auth_count) ELSE 0
	END as other_aux_time,
	CASE
		WHEN phone_aux_time > 1
		AND total_auth_count > 1 THEN (phone_aux_time / total_auth_count) ELSE 0
	END as phone_aux_all_auths,
	CASE
		WHEN bop.channel = 'PHONE'
		AND phone_aux_time > 1
		AND phone_auth_count > 1 THEN (phone_aux_time / phone_auth_count) ELSE 0
	END as phone_aux_phone_auths,
	CASE
		WHEN bop.channel = 'EMAIL'
		AND email_aux_time > 1
		AND email_auth_count > 1 THEN (email_aux_time / email_auth_count) ELSE 0
	END as email_aux_time,
	CASE
		WHEN bop.channel = 'FAX'
		AND fax_aux_time > 1
		AND fax_auth_count > 1 THEN (fax_aux_time / fax_auth_count) ELSE 0
	END as fax_aux_time,
	CASE
		WHEN bop.p2p_review_touches > 0
		AND p2p_aux_time > 1
		AND p2p_auth_count > 1 THEN (p2p_aux_time / p2p_auth_count) ELSE 0
	END as p2p_aux_time,
	CASE
		WHEN soowp.missing_info_attempts > 0
		AND missing_info_time > 1
		AND mi_auth_count > 1 THEN (missing_info_time / mi_auth_count) ELSE 0
	END as missing_info_time,
	gdl.guideline_interaction,
	nrd.first_review_ts,
	nrd.attachment_count,
	state.laststate_final,
	state.nextstate_final,
	rtl.*,
	raf.model,
	raf.final_raf,
	aq.*
FROM "tableau"."business_operation_production" bop
	LEFT JOIN "tableau"."service_request_production" srp on bop.cohere_auth_id = srp.cohereid
	LEFT JOIN "reference_library"."cms_rbcs_betos" betos on betos.hcpcs_cd = bop.primary_procedure_code
	LEFT JOIN (
		SELECT srp.cohereid,
			date(tat_due_date) as tat_due_date,
			CASE
				WHEN missing_info_call_number = '3rd attempt made' THEN 3
				WHEN missing_info_call_number = '2nd attempt made' THEN 2
				WHEN missing_info_call_number = '1st attempt made' THEN 1 ELSE 0
			END as missing_info_attempts
		FROM "tableau"."sf_service_ops_offline_work_production" srp --WHERE cohereid = 'AAGU6959'
	) soowp on soowp.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT srp.cohereid as cohereid,
			rrd.title as title,
			rrd.convene_rule_link as rule_link,
			rrd.actions_objectvalue as rule_value,
			rrd.rule_ran_sequence as rule_ran_sequence,
			rank() over (
				partition by srp.cohereid
				order by rrd.rule_ran_sequence asc
			) as rank
		FROM "tableau"."rule_run_description" rrd
			LEFT JOIN "tableau"."service_request_production" srp on rrd.servicerequestid = srp.id
		WHERE rrd.ruleruntiming = 'ON_SUBMISSION'
			AND rrd.actions_executed = TRUE
			AND rrd.actions_type = 'SetValue'
			AND rrd.active = TRUE
			AND rrd.actions_objectattribute = 'authStatus' --AND srp.cohereid IN ('PLXF7198','ACKK7233','DXII4329','SERP7729','GXKN4692')
		GROUP BY 1,
			2,
			3,
			4,
			5
	) rt on rt.cohereid = bop.cohere_auth_id
	and rt.rank = 1
	LEFT JOIN (
		SELECT DISTINCT srp.patient_coverage_lineofbusinesstype as lob,
			srp.primaryprocedurecode_code as dx,
			srp.encountertype as encountertype,
			--srp.patient_coverage_stateofissue,
			--srp.primarydiagnosis_description,
			--srp.primaryprocedurecode_code
			--srp.units, 
			--srp.encountertype
			--srp.carepathname,
			rrd.ruleid as ruleid,
			rrd.title as title,
			rrd.convene_rule_link as rule_link,
			--rrd.rule_datecreated as creation_date,
			--rrd.rule_ran_sequence as rule_value,
			count(distinct(srp.cohereid)) as auto_approved_auth_by_lobpx,
			rank() over (
				partition by srp.patient_coverage_lineofbusinesstype,
				srp.primaryprocedurecode_code,
				srp.encountertype --srp.patient_coverage_stateofissue,
				--srp.primarydiagnosis_description,
				--srp.primaryprocedurecode_code
				--srp.units, 
				--srp.encountertype
				--srp.carepathname,
				order by --rrd.rule_datecreated DESC, 
					count(distinct(srp.cohereid)) DESC
			) as rank
		FROM "tableau"."rule_run_description" rrd
			LEFT JOIN "tableau"."service_request_production" srp on rrd.servicerequestid = srp.id
		WHERE rrd.ruleruntiming = 'ON_SUBMISSION'
			AND rrd.actions_executed = TRUE
			AND rrd.actions_type = 'SetValue'
			AND rrd.active = TRUE
			AND rrd.actions_objectattribute = 'authStatus'
			AND rrd.actions_objectvalue = 'APPROVED' --AND srp.patient_coverage_lineofbusinesstype  = 'Medicare'
			--AND srp.primarydiagnosis_code = 'T85.848A'
			--AND srp.cohereid IN ('AHZB2702')
		GROUP BY 1,
			2,
			3,
			4,
			5,
			6
		HAVING count(distinct(srp.cohereid)) > 100
	) aa_rule on aa_rule.lob = srp.patient_coverage_lineofbusinesstype
	and aa_rule.dx = srp.primaryprocedurecode_code
	and srp.encountertype = aa_rule.encountertype
	and aa_rule.rank = 1
	LEFT JOIN (
		SELECT DISTINCT rp.servicerequest_cohereid as cohereid,
			SUM(
				CASE
					WHEN review_createdbyname IN (
						'Allison Jones',
						'Chelsea Fuhrer',
						'Chloe Crenshaw',
						'Deserae Furst',
						'Heather Wesolowski',
						'Hillary Clark',
						'Jessica Roarx',
						'Katherine Hydrick',
						'Kennedy Weir',
						'Kimberly Frazier',
						'Makenna Gorman',
						'Meagan Jones',
						'Petergayle Riley',
						'Rachael Bedell',
						'Ruqya Sirajuddin',
						'Stephanie Blair-Hicks',
						'Theresa Brokenshire',
						'Vadetra Woffordrole'
					)
					AND review_reviewtype = 'NurseReview'
					AND (
						review_nudgeattempted = 'true'
						OR nudgeresultedinchange = 'true'
					) THEN CAST(
						(
							CASE
								WHEN (rp.review_reviewdurationhrs > 1) THEN 1
								WHEN (rp.review_reviewdurationhrs < 0.05) THEN.05
								WHEN (
									rp.review_reviewdurationhrs > 0.05
									AND rp.review_reviewdurationhrs < 1
								) THEN rp.review_reviewdurationhrs ELSE 0
							END
						) * sf_rvw_mltp.multiplier as DECIMAL(10, 2)
					) ELSE 0
				END
			) AS sf_lpn_nudge_hours,
			SUM(
				CASE
					WHEN review_createdbyname IN (
						'Allison Jones',
						'Chelsea Fuhrer',
						'Chloe Crenshaw',
						'Deserae Furst',
						'Heather Wesolowski',
						'Hillary Clark',
						'Jessica Roarx',
						'Katherine Hydrick',
						'Kennedy Weir',
						'Kimberly Frazier',
						'Makenna Gorman',
						'Meagan Jones',
						'Petergayle Riley',
						'Rachael Bedell',
						'Ruqya Sirajuddin',
						'Stephanie Blair-Hicks',
						'Theresa Brokenshire',
						'Vadetra Woffordrole'
					)
					AND review_reviewtype = 'NurseReview'
					AND (
						review_nudgeattempted <> 'true'
						AND nudgeresultedinchange <> 'true'
					) THEN CAST(
						(
							CASE
								WHEN (rp.review_reviewdurationhrs > 1) THEN 1
								WHEN (rp.review_reviewdurationhrs < 0.05) THEN.05
								WHEN (
									rp.review_reviewdurationhrs > 0.05
									AND rp.review_reviewdurationhrs < 1
								) THEN rp.review_reviewdurationhrs ELSE 0
							END
						) * sf_rvw_mltp.multiplier as DECIMAL(10, 2)
					) ELSE 0
				END
			) AS sf_lpn_review_hours,
			SUM(
				CASE
					WHEN review_createdbyname NOT IN (
						'Allison Jones',
						'Chelsea Fuhrer',
						'Chloe Crenshaw',
						'Deserae Furst',
						'Heather Wesolowski',
						'Hillary Clark',
						'Jessica Roarx',
						'Katherine Hydrick',
						'Kennedy Weir',
						'Kimberly Frazier',
						'Makenna Gorman',
						'Meagan Jones',
						'Petergayle Riley',
						'Rachael Bedell',
						'Ruqya Sirajuddin',
						'Stephanie Blair-Hicks',
						'Theresa Brokenshire',
						'Vadetra Woffordrole'
					)
					AND review_reviewtype = 'NurseReview' THEN CAST(
						(
							CASE
								WHEN (rp.review_reviewdurationhrs > 1) THEN 1
								WHEN (rp.review_reviewdurationhrs < 0.05) THEN.05
								WHEN (
									rp.review_reviewdurationhrs > 0.05
									AND rp.review_reviewdurationhrs < 1
								) THEN rp.review_reviewdurationhrs ELSE 0
							END
						) * sf_rvw_mltp.multiplier as DECIMAL(10, 2)
					) ELSE 0
				END
			) AS sf_rn_review_hours,
			SUM(
				CASE
					WHEN review_reviewtype IN ('MdReview') THEN CAST(
						(
							CASE
								WHEN (rp.review_reviewdurationhrs > 1) THEN 1
								WHEN (rp.review_reviewdurationhrs < 0.05) THEN.05
								WHEN (
									rp.review_reviewdurationhrs > 0.05
									AND rp.review_reviewdurationhrs < 1
								) THEN rp.review_reviewdurationhrs ELSE 0
							END
						) * sf_rvw_mltp.multiplier as DECIMAL(10, 2)
					) ELSE 0
				END
			) AS sf_md_review_hours,
			SUM(
				CASE
					WHEN review_reviewtype IN ('PeerToPeerReview') THEN CAST(
						(
							CASE
								WHEN (rp.review_reviewdurationhrs > 1) THEN 1
								WHEN (rp.review_reviewdurationhrs < 0.05) THEN.05
								WHEN (
									rp.review_reviewdurationhrs > 0.05
									AND rp.review_reviewdurationhrs < 1
								) THEN rp.review_reviewdurationhrs ELSE 0
							END
						) * sf_rvw_mltp.multiplier as DECIMAL(10, 2)
					) ELSE 0
				END
			) AS sf_p2p_review_hours
		FROM "tableau"."review_production" rp
			LEFT JOIN (
				SELECT hrcmb.*,
					CASE
						WHEN sf_review_hours >.01
						and sr_review_hours >.01 THEN sf_review_hours / sr_review_hours ELSE 1
					END as multiplier
				FROM (
						SELECT DATE(review_datecreated) AS review_date,
							lower(rp.review_createdbyname) as reviewer_name,
							lower(sfrh.reviewer_email) as sf_reviewer_email,
							CAST(sfrh.sf_review_hours as DECIMAL(10, 2)) as sf_review_hours,
							CAST(
								SUM(
									CASE
										WHEN (
											rp.review_reviewtype IS NOT NULL
											AND rp.review_reviewdurationhrs > 1
										) THEN 1
										WHEN (
											rp.review_reviewtype IS NOT NULL
											AND rp.review_reviewdurationhrs < 0.05
										) THEN.05
										WHEN (
											rp.review_reviewtype IS NOT NULL
											AND rp.review_reviewdurationhrs > 0.05
											AND rp.review_reviewdurationhrs < 1
										) THEN rp.review_reviewdurationhrs ELSE 0
									END
								) as DECIMAL(10, 2)
							) as sr_review_hours
						FROM "tableau"."review_production" rp
							LEFT JOIN (
								SELECT DISTINCT review_createdbyname as reviewer_name,
									authoremail as reviewer_email
								FROM "tableau"."review_production" rp --LEFT JOIN "tableau"."sf_user_agent_summary_production"  sp on rp.authoremail = sp.user_email
								WHERE review_reviewstatus = 'COMPLETE'
									AND review_deleted IN ('false', ' ')
									AND lower(review_createdbyname) = lower(authorname)
							) nmcln on nmcln.reviewer_name = rp.review_createdbyname
							LEFT JOIN (
								SELECT DATE(createddate) AS review_date,
									lower(user_email) as reviewer_email,
									lower(user_name) as reviewer_name,
									(
										SUM(COALESCE(clinical_review, 0)) + SUM(COALESCE(spine, 0)) + SUM(COALESCE(ortho, 0)) + SUM(COALESCE(foot, 0)) + SUM(COALESCE(esi, 0)) + SUM(COALESCE(thpy, 0)) + SUM(COALESCE(nudges, 0)) + SUM(COALESCE(visco, 0)) + SUM(COALESCE(lpn_esi, 0)) + SUM(COALESCE(lpn_nudges, 0)) + SUM(COALESCE(lpn_visco_therapy, 0)) + SUM(COALESCE(md_peer_2_peer, 0))
									) AS sf_review_hours
								FROM "tableau"."sf_user_agent_summary_production"
								GROUP BY 1,
									2,
									3
							) sfrh on DATE(sfrh.review_date) = DATE(rp.review_datecreated)
							and (sfrh.reviewer_email) = lower(nmcln.reviewer_email)
						WHERE review_reviewstatus = 'COMPLETE'
							AND review_deleted IN ('false', ' ')
						GROUP BY 1,
							2,
							3,
							4
					) hrcmb
			) sf_rvw_mltp ON sf_rvw_mltp.review_date = DATE(rp.review_datecreated)
			AND sf_rvw_mltp.reviewer_name = lower(rp.review_createdbyname)
		GROUP BY 1
	) hrcmb on hrcmb.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT DATE(createddate) AS sf_date,
			--user_name,
			--CASE WHEN user_name IN ('Allison Jones','Chelsea Fuhrer','Chloe Crenshaw','Deserae Furst','Heather Wesolowski','Hillary Clark','Jessica Roarx','Katherine Hydrick','Kennedy Weir','Kimberly Frazier','Makenna Gorman','Meagan Jones','Petergayle Riley','Rachael Bedell','Ruqya Sirajuddin','Stephanie Blair-Hicks','Theresa Brokenshire','Vadetra Woffordrole') then 'LPN' else role end as user_role,
			(
				SUM(COALESCE(all_hands, 0)) + SUM(COALESCE(team_meeting, 0)) + SUM(COALESCE(x1x1, 0)) + SUM(COALESCE(self_guided_training, 0)) + SUM(COALESCE(training, 0))
			) as meetings_and_training_time,
			(
				(
					SUM(COALESCE(break, 0)) + SUM(COALESCE(lunch, 0))
				)
			) as non_production_time,
			(
				(
					SUM(COALESCE(bp, 0)) + SUM(COALESCE(cgx, 0)) + SUM(COALESCE(ncf, 0)) + SUM(COALESCE(fault_time, 0)) + SUM(COALESCE(park_time, 0)) + SUM(COALESCE(ievtstr, 0)) + SUM(COALESCE(project, 0)) + SUM(COALESCE(registration_outbound, 0)) + SUM(COALESCE(slt, 0)) + SUM(COALESCE(support, 0)) + SUM(COALESCE(system_down, 0)) + SUM(COALESCE(tat, 0)) + SUM(COALESCE(trgnh, 0)) + SUM(COALESCE(wvr, 0)) + SUM(COALESCE(qty, 0)) + SUM(COALESCE(pcp_pilot, 0)) --+ SUM(COALESCE(ready_for_outbound_time,0))
				)
			) as other_aux_time,
			(
				(
					SUM(COALESCE(hold_time, 0)) + SUM(COALESCE(inbound_call_time, 0)) + SUM(COALESCE(interrupted_time, 0)) + SUM(COALESCE(line_busy_time, 0)) + SUM(COALESCE(outbound_call_time, 0)) + SUM(COALESCE(ready_time, 0)) + SUM(COALESCE(voicemail, 0)) + SUM(COALESCE(no_answer_time, 0)) + SUM(COALESCE(no_answers_to_consult, 0)) + SUM(COALESCE(inbound_transfer_time, 0)) + SUM(COALESCE(wrap_up_time, 0))
				)
			) as phone_aux_time,
			(SUM(COALESCE(email, 0))) as email_aux_time,
			(SUM(COALESCE(fax, 0))) as fax_aux_time,
			(
				(
					SUM(COALESCE(p2p, 0)) + SUM(COALESCE(peer_to_peer_1st, 0)) + SUM(COALESCE(peer_to_peer_2nd, 0)) + SUM(COALESCE(peer_to_peer_3rd, 0)) + SUM(COALESCE(peer_to_peer_2nd, 0)) + SUM(COALESCE(portal_intake, 0))
				)
			) as p2p_aux_time,
			(
				(
					SUM(
						coalesce(cast(missing_info_1st as decimal(10, 2)), 0) / 60 / 60
					) + SUM(
						coalesce(cast(missing_info_2nd as decimal(10, 2)), 0) / 60 / 60
					) + SUM(
						coalesce(cast(missing_info_3rd as decimal(10, 2)), 0) / 60 / 60
					)
				)
			) as missing_info_time,
			fax_auth_count,
			email_auth_count,
			phone_auth_count,
			p2p_auth_count,
			total_auth_count,
			mi_auth_count
		FROM "tableau"."sf_user_agent_summary_production" sf_prd
			LEFT JOIN (
				SELECT DATE(submission_date) as submission_date,
					SUM(
						CASE
							WHEN channel = 'FAX' THEN 1 ELSE 0
						END
					) as fax_auth_count,
					SUM(
						CASE
							WHEN channel = 'EMAIL' THEN 1 ELSE 0
						END
					) as email_auth_count,
					SUM(
						CASE
							WHEN channel = 'PHONE' THEN 1 ELSE 0
						END
					) as phone_auth_count,
					SUM(
						CASE
							WHEN p2p_review_touches > 0 THEN 1 ELSE 0
						END
					) as p2p_auth_count,
					COUNT(DISTINCT(cohere_auth_id)) as total_auth_count
				FROM "tableau"."business_operation_production"
				GROUP BY 1
			) auths on auths.submission_date = DATE(sf_prd.createddate)
			LEFT JOIN (
				SELECT DATE(created_date_time) as date_time,
					COUNT(
						DISTINCT(
							CASE
								WHEN missing_info_call_number IS NOT NULL THEN (swp.cohereid) ELSE ''
							END
						)
					) as mi_auth_count
				FROM "tableau"."sf_service_ops_offline_work_production" swp
				GROUP BY 1
			) mi_auth on mi_auth.date_time = DATE(sf_prd.createddate) --WHERE DATE(createddate) = DATE('2021-10-05')
		GROUP BY 1,
			fax_auth_count,
			email_auth_count,
			phone_auth_count,
			p2p_auth_count,
			total_auth_count,
			mi_auth_count
		ORDER BY 1 DESC
	) sf_aux_hr on sf_aux_hr.sf_date = DATE(bop.submission_date)
	LEFT JOIN (
		SELECT DISTINCT servicerequest_cohereid,
			MAX(
				CASE
					WHEN "guidelineinfo_lcd_indications_checkedpreviouslychecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_lcd_indications_checkedpreviouslyunchecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_lcd_indications_uncheckedpreviouslychecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_lcd_reviewsubmissiondate" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_lcd_guidelineids" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_ncd_indications_checkedpreviouslychecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_ncd_indications_checkedpreviouslyunchecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_ncd_indications_uncheckedpreviouslychecked" IS NOT NULL THEN TRUE
					WHEN "guidelineinfo_ncd_guidelineids" IS NOT NULL THEN TRUE ELSE FALSE
				END
			) as guideline_interaction
		FROM "tableau"."review_production" --WHERE servicerequest_cohereid = 'AAGU6959'
		GROUP BY 1
		ORDER BY 1 DESC
	) gdl on gdl.servicerequest_cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT DISTINCT min_rn.servicerequest_cohereid as cohereid,
			minrn.min_date as first_review_ts,
			min_rn.servicerequest_attachmentscount as attachment_count
		FROM "tableau"."review_production" min_rn
			RIGHT JOIN (
				SELECT DISTINCT servicerequest_cohereid as ID,
					MIN(review_datecreated) as min_date
				FROM "tableau"."review_production"
				WHERE review_reviewtype IN ('NurseReview', 'PeerToPeerReview', 'MdReview')
					AND review_reviewstatus = 'COMPLETE' --AND servicerequest_cohereid = 'AAGU6959'
				GROUP BY 1
			) minrn ON minrn.ID = min_rn.servicerequest_cohereid
			AND minrn.min_date = min_rn.review_datecreated
			AND min_rn.review_createdbyname IS NOT NULL
	) nrd on nrd.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT DISTINCT max_rvw.servicerequest_cohereid as cohereid,
			max_rvw.laststate as laststate_final,
			max_rvw.nextstate as nextstate_final
		FROM "tableau"."review_production" max_rvw
			RIGHT JOIN (
				SELECT DISTINCT servicerequest_cohereid as ID,
					MAX(commitdateinstant) as max_date
				FROM "tableau"."review_production"
				WHERE laststate IS NOT NULL --AND servicerequest_cohereid = 'XPCB9854'
					AND author <> 'unknown'
				GROUP BY 1
			) maxrvw ON maxrvw.ID = max_rvw.servicerequest_cohereid
			AND maxrvw.max_date = max_rvw.commitdateinstant
			and max_rvw.laststate IS NOT NULL
			AND max_rvw.author <> 'unknown'
	) state on state.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT maxrtl.cohereid,
			touch.lpn_review_touch,
			touch.lpn_nudge_touch,
			touch.rn_touch,
			touch.md_touch,
			touch.p2p_touch,
			touch.sr_lpn_review_hours,
			touch.sr_lpn_nudge_hours,
			touch.sr_rn_review_hours,
			touch.sr_md_review_hours,
			touch.sr_p2p_review_hours,
			MAX(touch_threshold_exceeded) as touch_threshold_exceeded,
			MAX(review_date_1t) as review_date_1t,
			MAX(review_type_1t) as review_type_1t,
			MAX(review_outcome_1t) as review_outcome_1t,
			MAX(reviewer_1t) as reviewer_1t,
			MAX(review_duration_1t) as review_duration_1t,
			MAX(review_decision_1t) as review_decision_1t,
			MAX(review_determination_1t) as review_determination_1t,
			MAX(review_note_1t) as review_note_1t,
			MAX(nudge_detail_1t) as nudge_detail_1t,
			MAX(nudge_attempted_1t) as nudge_attempted_1t,
			MAX(nudge_accepted_1t) as nudge_accepted_1t,
			MAX(nudge_type_1t) as nudge_type_1t,
			MAX(review_date_2t) as review_date_2t,
			MAX(review_type_2t) as review_type_2t,
			MAX(review_outcome_2t) as review_outcome_2t,
			MAX(reviewer_2t) as reviewer_2t,
			MAX(review_duration_2t) as review_duration_2t,
			MAX(review_decision_2t) as review_decision_2t,
			MAX(review_determination_2t) as review_determination_2t,
			MAX(review_note_2t) as review_note_2t,
			MAX(nudge_detail_2t) as nudge_detail_2t,
			MAX(nudge_attempted_2t) as nudge_attempted_2t,
			MAX(nudge_accepted_2t) as nudge_accepted_2t,
			MAX(nudge_type_2t) as nudge_type_2t,
			MAX(review_date_3t) as review_date_3t,
			MAX(review_type_3t) as review_type_3t,
			MAX(review_outcome_3t) as review_outcome_3t,
			MAX(reviewer_3t) as reviewer_3t,
			MAX(review_duration_3t) as review_duration_3t,
			MAX(review_decision_3t) as review_decision_3t,
			MAX(review_determination_3t) as review_determination_3t,
			MAX(review_note_3t) as review_note_3t,
			MAX(nudge_detail_3t) as nudge_detail_3t,
			MAX(nudge_attempted_3t) as nudge_attempted_3t,
			MAX(nudge_accepted_3t) as nudge_accepted_3t,
			MAX(nudge_type_3t) as nudge_type_3t,
			MAX(review_date_4t) as review_date_4t,
			MAX(review_type_4t) as review_type_4t,
			MAX(review_outcome_4t) as review_outcome_4t,
			MAX(reviewer_4t) as reviewer_4t,
			MAX(review_duration_4t) as review_duration_4t,
			MAX(review_decision_4t) as review_decision_4t,
			MAX(review_determination_4t) as review_determination_4t,
			MAX(review_note_4t) as review_note_4t,
			MAX(nudge_detail_4t) as nudge_detail_4t,
			MAX(nudge_attempted_4t) as nudge_attempted_4t,
			MAX(nudge_accepted_4t) as nudge_accepted_4t,
			MAX(nudge_type_4t) as nudge_type_4t,
			MAX(review_date_5t) as review_date_5t,
			MAX(review_type_5t) as review_type_5t,
			MAX(review_outcome_5t) as review_outcome_5t,
			MAX(reviewer_5t) as reviewer_5t,
			MAX(review_duration_5t) as review_duration_5t,
			MAX(review_decision_5t) as review_decision_5t,
			MAX(review_determination_5t) as review_determination_5t,
			MAX(review_note_5t) as review_note_5t,
			MAX(nudge_detail_5t) as nudge_detail_5t,
			MAX(nudge_attempted_5t) as nudge_attempted_5t,
			MAX(nudge_accepted_5t) as nudge_accepted_5t,
			MAX(nudge_type_5t) as nudge_type_5t,
			MAX(review_date_6t) as review_date_6t,
			MAX(review_type_6t) as review_type_6t,
			MAX(review_outcome_6t) as review_outcome_6t,
			MAX(reviewer_6t) as reviewer_6t,
			MAX(review_duration_6t) as review_duration_6t,
			MAX(review_decision_6t) as review_decision_6t,
			MAX(review_determination_6t) as review_determination_6t,
			MAX(review_note_6t) as review_note_6t,
			MAX(nudge_detail_6t) as nudge_detail_6t,
			MAX(nudge_attempted_6t) as nudge_attempted_6t,
			MAX(nudge_accepted_6t) as nudge_accepted_6t,
			MAX(nudge_type_6t) as nudge_type_6t,
			MAX(review_date_7t) as review_date_7t,
			MAX(review_type_7t) as review_type_7t,
			MAX(review_outcome_7t) as review_outcome_7t,
			MAX(reviewer_7t) as reviewer_7t,
			MAX(review_duration_7t) as review_duration_7t,
			MAX(review_decision_7t) as review_decision_7t,
			MAX(review_determination_7t) as review_determination_7t,
			MAX(review_note_7t) as review_note_7t,
			MAX(nudge_detail_7t) as nudge_detail_7t,
			MAX(nudge_attempted_7t) as nudge_attempted_7t,
			MAX(nudge_accepted_7t) as nudge_accepted_7t,
			MAX(nudge_type_7t) as nudge_type_7t,
			MAX(review_date_8t) as review_date_8t,
			MAX(review_type_8t) as review_type_8t,
			MAX(review_outcome_8t) as review_outcome_8t,
			MAX(reviewer_8t) as reviewer_8t,
			MAX(review_duration_8t) as review_duration_8t,
			MAX(review_decision_8t) as review_decision_8t,
			MAX(review_determination_8t) as review_determination_8t,
			MAX(review_note_8t) as review_note_8t,
			MAX(nudge_detail_8t) as nudge_detail_8t,
			MAX(nudge_attempted_8t) as nudge_attempted_8t,
			MAX(nudge_accepted_8t) as nudge_accepted_8t,
			MAX(nudge_type_8t) as nudge_type_8t
		FROM (
				SELECT rtl.cohereid,
					CASE
						WHEN touch_rank = 1 THEN review_date ELSE NULL
					END as review_date_1t,
					CASE
						WHEN touch_rank = 1 THEN review_type ELSE NULL
					END as review_type_1t,
					CASE
						WHEN touch_rank = 1 THEN review_outcome ELSE NULL
					END as review_outcome_1t,
					CASE
						WHEN touch_rank = 1 THEN reviewer ELSE NULL
					END as reviewer_1t,
					CASE
						WHEN touch_rank = 1 THEN review_duration ELSE NULL
					END as review_duration_1t,
					CASE
						WHEN touch_rank = 1 THEN review_decision ELSE NULL
					END as review_decision_1t,
					CASE
						WHEN touch_rank = 1 THEN review_determination ELSE NULL
					END as review_determination_1t,
					CASE
						WHEN touch_rank = 1 THEN review_note ELSE NULL
					END as review_note_1t,
					CASE
						WHEN touch_rank = 1 THEN nudge_detail ELSE NULL
					END as nudge_detail_1t,
					CASE
						WHEN touch_rank = 1 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_1t,
					CASE
						WHEN touch_rank = 1 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_1t,
					CASE
						WHEN touch_rank = 1 THEN nudge_type ELSE NULL
					END as nudge_type_1t,
					CASE
						WHEN touch_rank = 2 THEN review_date ELSE NULL
					END as review_date_2t,
					CASE
						WHEN touch_rank = 2 THEN review_type ELSE NULL
					END as review_type_2t,
					CASE
						WHEN touch_rank = 2 THEN review_outcome ELSE NULL
					END as review_outcome_2t,
					CASE
						WHEN touch_rank = 2 THEN reviewer ELSE NULL
					END as reviewer_2t,
					CASE
						WHEN touch_rank = 2 THEN review_duration ELSE NULL
					END as review_duration_2t,
					CASE
						WHEN touch_rank = 2 THEN review_decision ELSE NULL
					END as review_decision_2t,
					CASE
						WHEN touch_rank = 2 THEN review_determination ELSE NULL
					END as review_determination_2t,
					CASE
						WHEN touch_rank = 2 THEN review_note ELSE NULL
					END as review_note_2t,
					CASE
						WHEN touch_rank = 2 THEN nudge_detail ELSE NULL
					END as nudge_detail_2t,
					CASE
						WHEN touch_rank = 2 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_2t,
					CASE
						WHEN touch_rank = 2 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_2t,
					CASE
						WHEN touch_rank = 2 THEN nudge_type ELSE NULL
					END as nudge_type_2t,
					CASE
						WHEN touch_rank = 3 THEN review_date ELSE NULL
					END as review_date_3t,
					CASE
						WHEN touch_rank = 3 THEN review_type ELSE NULL
					END as review_type_3t,
					CASE
						WHEN touch_rank = 3 THEN review_outcome ELSE NULL
					END as review_outcome_3t,
					CASE
						WHEN touch_rank = 3 THEN reviewer ELSE NULL
					END as reviewer_3t,
					CASE
						WHEN touch_rank = 3 THEN review_duration ELSE NULL
					END as review_duration_3t,
					CASE
						WHEN touch_rank = 3 THEN review_decision ELSE NULL
					END as review_decision_3t,
					CASE
						WHEN touch_rank = 3 THEN review_determination ELSE NULL
					END as review_determination_3t,
					CASE
						WHEN touch_rank = 3 THEN review_note ELSE NULL
					END as review_note_3t,
					CASE
						WHEN touch_rank = 3 THEN nudge_detail ELSE NULL
					END as nudge_detail_3t,
					CASE
						WHEN touch_rank = 3 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_3t,
					CASE
						WHEN touch_rank = 3 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_3t,
					CASE
						WHEN touch_rank = 3 THEN nudge_type ELSE NULL
					END as nudge_type_3t,
					CASE
						WHEN touch_rank = 4 THEN review_date ELSE NULL
					END as review_date_4t,
					CASE
						WHEN touch_rank = 4 THEN review_type ELSE NULL
					END as review_type_4t,
					CASE
						WHEN touch_rank = 4 THEN review_outcome ELSE NULL
					END as review_outcome_4t,
					CASE
						WHEN touch_rank = 4 THEN reviewer ELSE NULL
					END as reviewer_4t,
					CASE
						WHEN touch_rank = 4 THEN review_duration ELSE NULL
					END as review_duration_4t,
					CASE
						WHEN touch_rank = 4 THEN review_decision ELSE NULL
					END as review_decision_4t,
					CASE
						WHEN touch_rank = 4 THEN review_determination ELSE NULL
					END as review_determination_4t,
					CASE
						WHEN touch_rank = 4 THEN review_note ELSE NULL
					END as review_note_4t,
					CASE
						WHEN touch_rank = 4 THEN nudge_detail ELSE NULL
					END as nudge_detail_4t,
					CASE
						WHEN touch_rank = 4 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_4t,
					CASE
						WHEN touch_rank = 4 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_4t,
					CASE
						WHEN touch_rank = 4 THEN nudge_type ELSE NULL
					END as nudge_type_4t,
					CASE
						WHEN touch_rank = 5 THEN review_date ELSE NULL
					END as review_date_5t,
					CASE
						WHEN touch_rank = 5 THEN review_type ELSE NULL
					END as review_type_5t,
					CASE
						WHEN touch_rank = 5 THEN review_outcome ELSE NULL
					END as review_outcome_5t,
					CASE
						WHEN touch_rank = 5 THEN reviewer ELSE NULL
					END as reviewer_5t,
					CASE
						WHEN touch_rank = 5 THEN review_duration ELSE NULL
					END as review_duration_5t,
					CASE
						WHEN touch_rank = 5 THEN review_decision ELSE NULL
					END as review_decision_5t,
					CASE
						WHEN touch_rank = 5 THEN review_determination ELSE NULL
					END as review_determination_5t,
					CASE
						WHEN touch_rank = 5 THEN review_note ELSE NULL
					END as review_note_5t,
					CASE
						WHEN touch_rank = 5 THEN nudge_detail ELSE NULL
					END as nudge_detail_5t,
					CASE
						WHEN touch_rank = 5 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_5t,
					CASE
						WHEN touch_rank = 5 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_5t,
					CASE
						WHEN touch_rank = 5 THEN nudge_type ELSE NULL
					END as nudge_type_5t,
					CASE
						WHEN touch_rank = 6 THEN review_date ELSE NULL
					END as review_date_6t,
					CASE
						WHEN touch_rank = 6 THEN review_type ELSE NULL
					END as review_type_6t,
					CASE
						WHEN touch_rank = 6 THEN review_outcome ELSE NULL
					END as review_outcome_6t,
					CASE
						WHEN touch_rank = 6 THEN reviewer ELSE NULL
					END as reviewer_6t,
					CASE
						WHEN touch_rank = 6 THEN review_duration ELSE NULL
					END as review_duration_6t,
					CASE
						WHEN touch_rank = 6 THEN review_decision ELSE NULL
					END as review_decision_6t,
					CASE
						WHEN touch_rank = 6 THEN review_determination ELSE NULL
					END as review_determination_6t,
					CASE
						WHEN touch_rank = 6 THEN review_note ELSE NULL
					END as review_note_6t,
					CASE
						WHEN touch_rank = 6 THEN nudge_detail ELSE NULL
					END as nudge_detail_6t,
					CASE
						WHEN touch_rank = 6 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_6t,
					CASE
						WHEN touch_rank = 6 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_6t,
					CASE
						WHEN touch_rank = 6 THEN nudge_type ELSE NULL
					END as nudge_type_6t,
					CASE
						WHEN touch_rank = 7 THEN review_date ELSE NULL
					END as review_date_7t,
					CASE
						WHEN touch_rank = 7 THEN review_type ELSE NULL
					END as review_type_7t,
					CASE
						WHEN touch_rank = 7 THEN review_outcome ELSE NULL
					END as review_outcome_7t,
					CASE
						WHEN touch_rank = 7 THEN reviewer ELSE NULL
					END as reviewer_7t,
					CASE
						WHEN touch_rank = 7 THEN review_duration ELSE NULL
					END as review_duration_7t,
					CASE
						WHEN touch_rank = 7 THEN review_decision ELSE NULL
					END as review_decision_7t,
					CASE
						WHEN touch_rank = 7 THEN review_determination ELSE NULL
					END as review_determination_7t,
					CASE
						WHEN touch_rank = 7 THEN review_note ELSE NULL
					END as review_note_7t,
					CASE
						WHEN touch_rank = 7 THEN nudge_detail ELSE NULL
					END as nudge_detail_7t,
					CASE
						WHEN touch_rank = 7 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_7t,
					CASE
						WHEN touch_rank = 7 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_7t,
					CASE
						WHEN touch_rank = 7 THEN nudge_type ELSE NULL
					END as nudge_type_7t,
					CASE
						WHEN touch_rank = 8 THEN review_date ELSE NULL
					END as review_date_8t,
					CASE
						WHEN touch_rank = 8 THEN review_type ELSE NULL
					END as review_type_8t,
					CASE
						WHEN touch_rank = 8 THEN review_outcome ELSE NULL
					END as review_outcome_8t,
					CASE
						WHEN touch_rank = 8 THEN reviewer ELSE NULL
					END as reviewer_8t,
					CASE
						WHEN touch_rank = 8 THEN review_duration ELSE NULL
					END as review_duration_8t,
					CASE
						WHEN touch_rank = 8 THEN review_decision ELSE NULL
					END as review_decision_8t,
					CASE
						WHEN touch_rank = 8 THEN review_determination ELSE NULL
					END as review_determination_8t,
					CASE
						WHEN touch_rank = 8 THEN review_note ELSE NULL
					END as review_note_8t,
					CASE
						WHEN touch_rank = 8 THEN nudge_detail ELSE NULL
					END as nudge_detail_8t,
					CASE
						WHEN touch_rank = 8 THEN nudge_attempted ELSE NULL
					END as nudge_attempted_8t,
					CASE
						WHEN touch_rank = 8 THEN nudge_accepted ELSE NULL
					END as nudge_accepted_8t,
					CASE
						WHEN touch_rank = 8 THEN nudge_type ELSE NULL
					END as nudge_type_8t,
					CASE
						WHEN touch_rank > 8 THEN TRUE ELSE FALSE
					END touch_threshold_exceeded
				FROM (
						SELECT rank() over (
								partition by servicerequest_cohereid
								order by review_datecreated
							) as touch_rank,
							servicerequest_cohereid as cohereid,
							review_datecreated as review_date,
							CASE
								WHEN review_createdbyname IN (
									'Allison Jones',
									'Chelsea Fuhrer',
									'Chloe Crenshaw',
									'Deserae Furst',
									'Heather Wesolowski',
									'Hillary Clark',
									'Jessica Roarx',
									'Katherine Hydrick',
									'Kennedy Weir',
									'Kimberly Frazier',
									'Makenna Gorman',
									'Meagan Jones',
									'Petergayle Riley',
									'Rachael Bedell',
									'Ruqya Sirajuddin',
									'Stephanie Blair-Hicks',
									'Theresa Brokenshire',
									'Vadetra Woffordrole'
								)
								AND review_reviewtype = 'NurseReview'
								AND (
									review_nudgeattempted = 'true'
									OR nudgeresultedinchange = 'true'
								) THEN 'LPN - Nudge'
								WHEN review_createdbyname IN (
									'Allison Jones',
									'Chelsea Fuhrer',
									'Chloe Crenshaw',
									'Deserae Furst',
									'Heather Wesolowski',
									'Hillary Clark',
									'Jessica Roarx',
									'Katherine Hydrick',
									'Kennedy Weir',
									'Kimberly Frazier',
									'Makenna Gorman',
									'Meagan Jones',
									'Petergayle Riley',
									'Rachael Bedell',
									'Ruqya Sirajuddin',
									'Stephanie Blair-Hicks',
									'Theresa Brokenshire',
									'Vadetra Woffordrole'
								)
								AND review_reviewtype = 'NurseReview'
								AND (
									review_nudgeattempted <> 'true'
									AND nudgeresultedinchange <> 'true'
								) THEN 'LPN - Review' ELSE review_reviewtype
							END as review_type,
							review_reviewoutcome as review_outcome,
							review_criteriadecision as criteria_decision,
							review_createdbyname as reviewer,
							review_reviewdurationhrs as review_duration,
							review_determinationnote as review_determination,
							review_decisionreasoning as review_decision,
							review_authorizationnote as review_note,
							review_nudgedescription as nudge_detail,
							review_nudgeattempted as nudge_attempted,
							nudgeresultedinchange as nudge_accepted,
							nudgetypes as nudge_type
						FROM "tableau"."review_production" rtl
						WHERE --servicerequest_cohereid IN ('AAGU6959') ANDx
							review_reviewstatus = 'COMPLETE'
							AND review_deleted IN ('false', ' ')
						ORDER BY servicerequest_cohereid,
							review_datecreated
					) rtl
			) maxrtl
			LEFT JOIN (
				SELECT abc.cohereid,
					COUNT(
						DISTINCT(
							CASE
								WHEN abc.review_type = 'LPN - Review' THEN abc.review_date ELSE NULL
							END
						)
					) as lpn_review_touch,
					COUNT(
						DISTINCT(
							CASE
								WHEN abc.review_type = 'LPN - Nudge' THEN abc.review_date ELSE NULL
							END
						)
					) as lpn_nudge_touch,
					COUNT(
						DISTINCT(
							CASE
								WHEN abc.review_type = 'NurseReview' THEN abc.review_date ELSE NULL
							END
						)
					) as rn_touch,
					COUNT(
						DISTINCT(
							CASE
								WHEN abc.review_type = 'MdReview' THEN abc.review_date ELSE NULL
							END
						)
					) as md_touch,
					COUNT(
						DISTINCT(
							CASE
								WHEN abc.review_type = 'PeerToPeerReview' THEN abc.review_date ELSE NULL
							END
						)
					) as p2p_touch,
					SUM(
						CASE
							WHEN abc.review_type = 'LPN - Review' THEN CASE
								WHEN (
									abc.review_type IS NOT NULL
									AND abc.review_duration > 1
								) THEN 1
								WHEN (
									abc.review_type IS NOT NULL
									AND abc.review_duration < 0.05
								) THEN.05
								WHEN (
									abc.review_type IS NOT NULL
									AND abc.review_duration > 0.05
									AND abc.review_duration < 1
								) THEN abc.review_duration ELSE 0
							END ELSE 0
						END
					) as sr_lpn_review_hours,
					SUM(
						CASE
							WHEN review_type = 'LPN - Nudge' THEN CASE
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 1
								) THEN 1
								WHEN (
									review_type IS NOT NULL
									AND review_duration < 0.05
								) THEN.05
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 0.05
									AND review_duration < 1
								) THEN review_duration ELSE 0
							END ELSE 0
						END
					) as sr_lpn_nudge_hours,
					SUM(
						CASE
							WHEN review_type = 'NurseReview' THEN CASE
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 1
								) THEN 1
								WHEN (
									review_type IS NOT NULL
									AND review_duration < 0.05
								) THEN.05
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 0.05
									AND review_duration < 1
								) THEN review_duration ELSE 0
							END ELSE 0
						END
					) as sr_rn_review_hours,
					SUM(
						CASE
							WHEN review_type = 'MdReview' THEN CASE
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 1
								) THEN 1
								WHEN (
									review_type IS NOT NULL
									AND review_duration < 0.05
								) THEN.05
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 0.05
									AND review_duration < 1
								) THEN review_duration ELSE 0
							END ELSE 0
						END
					) as sr_md_review_hours,
					SUM(
						CASE
							WHEN review_type = 'PeerToPeerReview' THEN CASE
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 1
								) THEN 1
								WHEN (
									review_type IS NOT NULL
									AND review_duration < 0.05
								) THEN.05
								WHEN (
									review_type IS NOT NULL
									AND review_duration > 0.05
									AND review_duration < 1
								) THEN review_duration ELSE 0
							END ELSE 0
						END
					) as sr_p2p_review_hours
				FROM (
						SELECT rank() over (
								partition by servicerequest_cohereid
								order by review_datecreated
							) as touch_rank,
							servicerequest_cohereid as cohereid,
							review_datecreated as review_date,
							CASE
								WHEN review_createdbyname IN (
									'Allison Jones',
									'Chelsea Fuhrer',
									'Chloe Crenshaw',
									'Deserae Furst',
									'Heather Wesolowski',
									'Hillary Clark',
									'Jessica Roarx',
									'Katherine Hydrick',
									'Kennedy Weir',
									'Kimberly Frazier',
									'Makenna Gorman',
									'Meagan Jones',
									'Petergayle Riley',
									'Rachael Bedell',
									'Ruqya Sirajuddin',
									'Stephanie Blair-Hicks',
									'Theresa Brokenshire',
									'Vadetra Woffordrole'
								)
								AND review_reviewtype = 'NurseReview'
								AND (
									review_nudgeattempted = 'true'
									OR nudgeresultedinchange = 'true'
								) THEN 'LPN - Nudge'
								WHEN review_createdbyname IN (
									'Allison Jones',
									'Chelsea Fuhrer',
									'Chloe Crenshaw',
									'Deserae Furst',
									'Heather Wesolowski',
									'Hillary Clark',
									'Jessica Roarx',
									'Katherine Hydrick',
									'Kennedy Weir',
									'Kimberly Frazier',
									'Makenna Gorman',
									'Meagan Jones',
									'Petergayle Riley',
									'Rachael Bedell',
									'Ruqya Sirajuddin',
									'Stephanie Blair-Hicks',
									'Theresa Brokenshire',
									'Vadetra Woffordrole'
								)
								AND review_reviewtype = 'NurseReview'
								AND (
									review_nudgeattempted <> 'true'
									AND nudgeresultedinchange <> 'true'
								) THEN 'LPN - Review' ELSE review_reviewtype
							END as review_type,
							review_reviewoutcome as review_outcome,
							review_createdbyname as reviewer,
							review_reviewdurationhrs as review_duration,
							review_decisionreasoning as review_decision
						FROM "tableau"."review_production" touch
						WHERE --servicerequest_cohereid IN ('CREW5649','AAGU6959') AND
							review_reviewstatus = 'COMPLETE'
							AND review_deleted IN ('false', ' ')
						ORDER BY servicerequest_cohereid,
							review_datecreated
					) abc
				GROUP BY 1
			) touch on touch.cohereid = maxrtl.cohereid
		GROUP BY 1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9,
			10,
			11
	) rtl on rtl.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT hcc.cohereid,
			hcc.model,
			hcc.final_raf
		FROM "tableau"."hcc_auth_uat" hcc
	) raf on raf.cohereid = bop.cohere_auth_id
	LEFT JOIN (
		SELECT sr_id,
			MAX(touch_rank) as number_of_questions_answered,
			MAX(question_1) as question_1,
			MAX(question_type_1) as question_type_1,
			MAX(response_1) as response_1,
			MAX(question_2) as question_2,
			MAX(question_type_2) as question_type_2,
			MAX(response_2) as response_2,
			MAX(question_3) as question_3,
			MAX(question_type_3) as question_type_3,
			MAX(response_3) as response_3,
			MAX(question_4) as question_4,
			MAX(question_type_4) as question_type_4,
			MAX(response_4) as response_4,
			MAX(question_5) as question_5,
			MAX(question_type_5) as question_type_5,
			MAX(response_5) as response_5,
			MAX(question_6) as question_6,
			MAX(question_type_6) as question_type_6,
			MAX(response_6) as response_6,
			MAX(question_7) as question_7,
			MAX(question_type_7) as question_type_7,
			MAX(response_7) as response_7,
			MAX(question_8) as question_8,
			MAX(question_type_8) as question_type_8,
			MAX(response_8) as response_8,
			MAX(question_9) as question_9,
			MAX(question_type_9) as question_type_9,
			MAX(response_9) as response_9,
			MAX(question_10) as question_10,
			MAX(question_type_10) as question_type_10,
			MAX(response_10) as response_10,
			MAX(question_11) as question_11,
			MAX(question_type_11) as question_type_11,
			MAX(response_11) as response_11,
			MAX(question_12) as question_12,
			MAX(question_type_12) as question_type_12,
			MAX(response_12) as response_12
		FROM(
				SELECT sr_id,
					touch_rank,
					CASE
						WHEN touch_rank = 1 THEN questiontext ELSE NULL
					END as question_1,
					CASE
						WHEN touch_rank = 1 THEN type ELSE NULL
					END as question_type_1,
					CASE
						WHEN touch_rank = 1 THEN responses ELSE NULL
					END as response_1,
					CASE
						WHEN touch_rank = 2 THEN questiontext ELSE NULL
					END as question_2,
					CASE
						WHEN touch_rank = 2 THEN type ELSE NULL
					END as question_type_2,
					CASE
						WHEN touch_rank = 2 THEN responses ELSE NULL
					END as response_2,
					CASE
						WHEN touch_rank = 3 THEN questiontext ELSE NULL
					END as question_3,
					CASE
						WHEN touch_rank = 3 THEN type ELSE NULL
					END as question_type_3,
					CASE
						WHEN touch_rank = 3 THEN responses ELSE NULL
					END as response_3,
					CASE
						WHEN touch_rank = 4 THEN questiontext ELSE NULL
					END as question_4,
					CASE
						WHEN touch_rank = 4 THEN type ELSE NULL
					END as question_type_4,
					CASE
						WHEN touch_rank = 4 THEN responses ELSE NULL
					END as response_4,
					CASE
						WHEN touch_rank = 5 THEN questiontext ELSE NULL
					END as question_5,
					CASE
						WHEN touch_rank = 5 THEN type ELSE NULL
					END as question_type_5,
					CASE
						WHEN touch_rank = 5 THEN responses ELSE NULL
					END as response_5,
					CASE
						WHEN touch_rank = 6 THEN questiontext ELSE NULL
					END as question_6,
					CASE
						WHEN touch_rank = 6 THEN type ELSE NULL
					END as question_type_6,
					CASE
						WHEN touch_rank = 6 THEN responses ELSE NULL
					END as response_6,
					CASE
						WHEN touch_rank = 7 THEN questiontext ELSE NULL
					END as question_7,
					CASE
						WHEN touch_rank = 7 THEN type ELSE NULL
					END as question_type_7,
					CASE
						WHEN touch_rank = 7 THEN responses ELSE NULL
					END as response_7,
					CASE
						WHEN touch_rank = 8 THEN questiontext ELSE NULL
					END as question_8,
					CASE
						WHEN touch_rank = 8 THEN type ELSE NULL
					END as question_type_8,
					CASE
						WHEN touch_rank = 8 THEN responses ELSE NULL
					END as response_8,
					CASE
						WHEN touch_rank = 9 THEN questiontext ELSE NULL
					END as question_9,
					CASE
						WHEN touch_rank = 9 THEN type ELSE NULL
					END as question_type_9,
					CASE
						WHEN touch_rank = 9 THEN responses ELSE NULL
					END as response_9,
					CASE
						WHEN touch_rank = 10 THEN questiontext ELSE NULL
					END as question_10,
					CASE
						WHEN touch_rank = 10 THEN type ELSE NULL
					END as question_type_10,
					CASE
						WHEN touch_rank = 10 THEN responses ELSE NULL
					END as response_10,
					CASE
						WHEN touch_rank = 11 THEN questiontext ELSE NULL
					END as question_11,
					CASE
						WHEN touch_rank = 11 THEN type ELSE NULL
					END as question_type_11,
					CASE
						WHEN touch_rank = 11 THEN responses ELSE NULL
					END as response_11,
					CASE
						WHEN touch_rank = 12 THEN questiontext ELSE NULL
					END as question_12,
					CASE
						WHEN touch_rank = 12 THEN type ELSE NULL
					END as question_type_12,
					CASE
						WHEN touch_rank = 12 THEN responses ELSE NULL
					END as response_12
				FROM (
						SELECT rank() over (
								partition by servicerequestid
								order by dateanswered_utc,
									questionid
							) as touch_rank,
							servicerequestid as sr_id,
							questiontext,
							type,
							responses
						FROM "tableau"."assessment_response_production" --WHERE servicerequestid='5fe0be12f1f2da000144608e'
					)
			)
		GROUP BY 1
	) aq on aq.sr_id = srp.id
