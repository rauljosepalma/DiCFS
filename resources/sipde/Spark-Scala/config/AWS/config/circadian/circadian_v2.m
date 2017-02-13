inputs=AMIGO_default_options();

%======================
% MODEL RELATED DATA
%======================
inputs.model.input_model_type='charmodelC';
inputs.model.n_st=7;                                     
inputs.model.n_par=27;                                   
inputs.model.n_stimulus=1;                               
inputs.model.st_names=char('CL_m','CL_c','CL_n','CT_m','CT_c','CT_n','CP_n');    

inputs.model.par_names=char('n1','n2','g1','g2','m1','m2','m3','m4','m5','m6',...
    'm7','k1','k2','k3','k4','k5','k6','k7','p1','p2',...
    'p3','r1','r2','r3','r4','q1','q2');             

inputs.model.stimulus_names=char('light');                

inputs.model.eqns=...                                      
    char('dCL_m=q1*CP_n*light+n1*CT_n/(g1+CT_n)-m1*CL_m/(k1+CL_m)',...
    'dCL_c=p1*CL_m-r1*CL_c+r2*CL_n-m2*CL_c/(k2+CL_c)',...
    'dCL_n=r1*CL_c-r2*CL_n-m3*CL_n/(k3+CL_n)',...
    'dCT_m=n2*g2^2/(g2^2+CL_n^2)-m4*CT_m/(k4+CT_m)',...
    'dCT_c=p2*CT_m-r3*CT_c+r4*CT_n-m5*CT_c/(k5+CT_c)',...
    'dCT_n=r3*CT_c-r4*CT_n-m6*CT_n/(k6+CT_n)',...
    'dCP_n=(1-light)*p3-m7*CP_n/(k7+CP_n)-q2*light*CP_n');

inputs.model.par=[7.5038 0.6801 1.4992 3.0412 10.0982...   
    1.9685 3.7511 2.3422 7.2482 1.8981 1.2 3.8045...
    5.3087 4.1946 2.5356 1.4420 4.8600 1.2 2.1994...
    9.4440 0.5 0.2817 0.7676 0.4364 7.3021 4.5703 1.0]; 

%==================================
% EXPERIMENTAL SCHEME RELATED DATA
%==================================
inputs.exps.n_exp=2;                                  
for iexp=1:inputs.exps.n_exp
    inputs.exps.exp_y0{iexp}=zeros(1,inputs.model.n_st);  
    inputs.exps.t_f{iexp}=120;                            
   
    
    % OBSEVABLES DEFINITION
    inputs.exps.n_obs{iexp}=2;                            
    inputs.exps.obs_names{iexp}=char('Lum','mRNAa');      
    inputs.exps.obs{iexp}=char('Lum=CL_m','mRNAa=CT_m');   
end


inputs.exps.u_interp{1}='sustained';                  

inputs.exps.t_con{1}=[0 120];                         
inputs.exps.u{1}=1;                                 

inputs.exps.u_interp{2}='pulse-down';                 
inputs.exps.n_pulses{2}=5;                            
inputs.exps.u_min{2}=0;inputs.exps.u_max{2}=1;        
inputs.exps.t_con{2}=0:12:120;                    
 
inputs.exps.n_s{1}=15;
inputs.exps.n_s{2}=25;

%==================================
% EXPERIMENTAL DATA RELATED INFO
%==================================
inputs.exps.n_s{1}=15;                                
inputs.exps.n_s{2}=25;                                

inputs.exps.data_type='real';                         
inputs.exps.noise_type='homo_var';                               

%inputs.exps.exp_data{1}=[                              
%    0.037642  0.059832
%    1.398618  0.983442
%     1.606762  0.433379
%    0.265345  0.628819
%    1.417288  0.858973
%    1.381613  0.496637
%    0.504584  0.717923
%    1.240249  0.862584
%    1.180193  0.634508
%    0.775945  0.679648
%    1.514514  0.735783
%    0.904653  0.593644
%    0.753736  0.759013
%    1.389312  0.678665
%    0.833228  0.574736
%    ];

%inputs.exps.error_data{1}=[                            
%    0.037642  0.059832
%    0.072461  0.013999
%    0.002877  0.020809
%    0.050324  0.002705
%    0.042936  0.017832
%    0.044338  0.022538
%    0.016335  0.017981
%    0.164745  0.035301
%    0.010631  0.102381
%    0.127745  0.065791
%    0.081671  0.049568
%    0.126739  0.050306
%    0.006308  0.018894
%    0.054665  0.066953
%    0.082163  0.015295
%    ];

%inputs.exps.exp_data{2}=[
%    0.146016  0.018152
%    0.831813  1.002499
%    1.874870  0.816779
%    1.927580  0.544111
%    1.139536  0.354476
%    0.876938  0.520424
%    0.559600  0.802322
%    1.273548  0.939453
%    1.696482  0.687495
%    1.065496  0.577896
%    0.847460  0.524076
%    0.517520  0.738095
%    1.162232  0.826737
%    1.421504  0.779833
%    1.340639  0.550493
%    0.563822  0.515605
%    0.402755  0.714877
%    1.029856  0.871118
%    1.490741  0.840174
%    1.580873  0.692047
%    0.696610  0.459481
%    0.141546  0.646803
%    0.804194  0.925806
%    1.622378  0.824711
%    1.525194  0.537398
%    ];

%inputs.exps.error_data{2}=[
%    0.146016  0.018152
%    0.066547  0.045194
%    0.184009  0.101495
%    0.047431  0.030858
%    0.175280  0.033712
%    0.031945  0.048733
%    0.107148  0.008715
%    0.019847  0.072804
%    0.111892  0.001840
%    0.104932  0.058752
%    0.059721  0.033324
%    0.056537  0.000360
%    0.051815  0.037473
%    0.103393  0.028094
%    0.008084  0.012024
%    0.188444  0.022982
%    0.046354  0.031981
%    0.043436  0.003749
%    0.030177  0.042560
%    0.116245  0.110535
%    0.059345  0.025112
%    0.218587  0.000564
%    0.115783  0.043708
%    0.099239  0.002678
%%    0.010644  0.052990
%    ];

% quitándolle o error añadido

inputs.exps.exp_data{1}=[                              
         0         0
    1.4711    0.9694
    1.6039    0.4542
    0.3157    0.6261
    1.3744    0.8768
    1.3373    0.5192
    0.5209    0.6999
    1.4050    0.8273
    1.1696    0.5321
    0.6482    0.7454
    1.4328    0.7854
    1.0314    0.5433
    0.7600    0.7779
    1.4440    0.7456
    0.9154    0.5594
    ];

inputs.exps.error_data{1}=[                            
    0.037642  0.059832
    0.072461  0.013999
    0.002877  0.020809
    0.050324  0.002705
    0.042936  0.017832
    0.044338  0.022538
    0.016335  0.017981
    0.164745  0.035301
    0.010631  0.102381
    0.127745  0.065791
    0.081671  0.049568
    0.126739  0.050306
    0.006308  0.018894
    0.054665  0.066953
    0.082163  0.015295
    ];

inputs.exps.exp_data{2}=[
         0         0
    0.7653    0.9573
    1.6909    0.9183
    1.8801    0.5750
    0.9643    0.3882
    0.9089    0.5692
    0.6667    0.8110
    1.2934    0.8666
    1.5846    0.6893
    1.1704    0.5191
    0.7877    0.5574
    0.5741    0.7377
    1.1104    0.8642
    1.5249    0.7517
    1.3326    0.5625
    0.7523    0.4926
    0.4491    0.6829
    0.9864    0.8749
    1.5209    0.7976
    1.4646    0.5815
    0.7560    0.4344
    0.3601    0.6474
    0.9200    0.8821
    1.5231    0.8220
    1.5358    0.5904
    ];

inputs.exps.error_data{2}=[
    0.146016  0.018152
    0.066547  0.045194
    0.184009  0.101495
    0.047431  0.030858
    0.175280  0.033712
    0.031945  0.048733
    0.107148  0.008715
    0.019847  0.072804
    0.111892  0.001840
    0.104932  0.058752
    0.059721  0.033324
    0.056537  0.000360
    0.051815  0.037473
    0.103393  0.028094
    0.008084  0.012024
    0.188444  0.022982
    0.046354  0.031981
    0.043436  0.003749
    0.030177  0.042560
    0.116245  0.110535
    0.059345  0.025112
    0.218587  0.000564
    0.115783  0.043708
    0.099239  0.002678
    0.010644  0.052990
    ];

inputs.PEsol.id_global_theta=char('n1','n2','m1','m4','m6','m7','k1','k4','p3'); 

inputs.PEsol.global_theta_guess=[...
    6.09794
    0.710144
    8.92735
    2.38504
    2.11024
    1.29912
    4.32079
    2.45277
    0.493076]';

inputs.PEsol.global_theta_max=100.*inputs.PEsol.global_theta_guess; 
inputs.PEsol.global_theta_min=0.01.*inputs.PEsol.global_theta_guess;


inputs.PEsol.id_local_theta_y0{1}=char('CL_c','CP_n');   
inputs.PEsol.local_theta_y0_max{1}=[1 1];           
inputs.PEsol.local_theta_y0_min{1}=[0 0];           
inputs.PEsol.local_theta_y0_guess{1}=[0.5 0.5];         

inputs.PEsol.id_local_theta_y0{2}=char('CL_c','CP_n');   
inputs.PEsol.local_theta_y0_max{2}=[1 1];               
inputs.PEsol.local_theta_y0_min{2}=[0 0];              
inputs.PEsol.local_theta_y0_guess{2}=[0.5 0.5];         
                                
%==================================
% COST FUNCTION RELATED DATA
%==================================

inputs.PEsol.PEcost_type='lsq';
inputs.PEsol.lsq_type='Q_I'; 

inputs.exps.index_observables{1}=[1 4];
inputs.exps.index_observables{2}=[1 4];

AMIGO_Prep(inputs);

AMIGO_SModel(inputs);

inputs.model.exe_type='fullC';

inputs.model.odes_file=fullfile(pwd,'fullC.c');

[inputs privstruct]=AMIGO_Prep(inputs);

save('load_C','-v7.3','inputs','privstruct');



