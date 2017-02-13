%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%nfkb_experimental_scheme.m
%
%  The experimental scheme available from:
% >  Lee E, Boone D, Chai S, Libby S, Chien M, Lodolce J, Ma A: Failure to
%    regulate TNF-induced NF-B and cell death responses in A20-deficient
%    mice. Science 2000, 289:2350-2354. 
% >  Hoffmann A, Levchenko A, Scott M, Baltimore D: The IkB-NF-kB signaling
%    module: temporal control and selective gene activation. Science 2002,
%    298:1241-1245.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%        INPUT FILE FOR THE EXPERIMENTAL SCHEME
%           > Experimental scheme: n_exp; exp_y0{iexp}; t_f{iexp}; 
%                                  u_interp{iexp}; t_con{iexp}; u{iexp}
%                                  n_obs{iexp}; obs_names{iexp}; obs{iexp} 
%                (AMIGO_SData)==>> n_s{iexp}; t_s{iexp}; 
%                                  data_type; noise_type; std_dev{iexp}                            
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%==================================
% EXPERIMENTAL SCHEME RELATED DATA
%==================================

inputs.exps.n_exp=1;  

% EXPERIMENT 1
inputs.exps.n_obs{1}=6; 
inputs.exps.obs_names{1}=char('NFkB_n','TIkBa_c','A20mRNA','TIKK','IKK_a','IkBa_t');% names of observables
inputs.exps.obs{1}=char('NFkB_n=NFkBn',...
                            'TIkBa_c=IkBa+IkBaNFkB',...
                            'A20mRNA=A20t',...
                            'TIKK=IKKn+IKKa+IKKi',...
                            'IKK_a=IKKa',...
                            'IkBa_t=IkBat');
inputs.exps.exp_y0{1}= [0.2  0 0 0 0  3.155e-004 2.2958e-003 ...
                 4.78285e-003 2.8697e-006 2.50663e-003...
                 3.43573e-003 2.86971e-006 0.06 ...
                 7.888e-005 2.86971e-006];  
             
inputs.exps.t_f{1}=3*3600;  

inputs.exps.u_interp{1}='sustained';                  %Stimuli definition for experiment 1:
                                                       %OPTIONS:u_interp: 'sustained' |'step'|'linear'(default)|'pulse-up'|'pulse-down' 
inputs.exps.t_con{1}=[0 3*3600];                         % Input swithching times: Initial and final time    
inputs.exps.u{1}=[1];     
inputs.exps.n_s{1}=12;                                % [] Number of sampling times for each experiment.
inputs.exps.t_s{1}=60.*[0 5 15 30 45 60 75 90 105 120 150 180]; % Sampling times

% EXPERIMENT 2
inputs.exps.n_obs{2}=6; 
inputs.exps.obs_names{2}=char('NFkB_n','TIkBa_c','A20mRNA','TIKK','IKK_a','IkBa_t'); % names of observables
inputs.exps.obs{2}=char('NFkB_n=NFkBn',...
                            'TIkBa_c=IkBa+IkBaNFkB',...
                            'A20mRNA=A20t',...
                            'TIKK=IKKn+IKKa+IKKi',...
                            'IKK_a=IKKa',...
                            'IkBa_t=IkBat');
inputs.exps.exp_y0{2}= [0.2  0 0 0 0  3.155e-004 2.2958e-003 ...
                 4.78285e-003 2.8697e-006 2.50663e-003...
                 3.43573e-003 2.86971e-006 0.06 ...
                 7.888e-005 2.86971e-006];  
             
inputs.exps.t_f{2}=3*3600;  

inputs.exps.u_interp{2}='pulse-down';                 %Stimuli definition for experiment 2
inputs.exps.n_pulses{2}=1;                            %Number of pulses |-|_
inputs.exps.u_min{2}=0;inputs.exps.u_max{2}=1;        %Minimum and maximum value for the input
inputs.exps.t_con{2}=[0 180 3*3600];                %Times of switching: Initial time, Intermediate times, Final time
inputs.exps.n_s{2}=12;                              % [] Number of sampling times for each experiment.
inputs.exps.t_s{2}=60.*[0 5 15 30 45 60 75 90 105 120 150 180];% Sampling times
 
