%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% TITLE: The NFKB module
%
%  The model considered in this work was proposed by Lipniacki et al.
%  Lipniacki et al 2004.
%  Lipniacki T, Paszek P, Brasier A, Luxon B, Kimmel M: Mathematical model of
%  NFkB regulatory module. J Theor Biol 2004, 228:195-215.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%         INPUT FILE TO GENERATE PSEUDO-EXPERIMENTAL DATA (USEFUL FOR
%        NUMERICAL EXAMPLES)
%
%        This is the minimum input file to generate pseudo-experimental data 
%        Default values are assigned to non defined inputs.
%
%        Minimum required inputs:
%           > Paths related data
%           > Model:               model_type; n_st; n_par; n_stimulus; 
%                                  st_names; par_names; stimulus_names;  
%                                  eqns; par
%           > Experimental scheme: n_exp; exp_y0{iexp}; t_f{iexp}; 
%                                  u_interp{iexp}; t_con{iexp}; u{iexp}
%                                  n_obs{iexp}; obs_names{iexp}; obs{iexp} 
%
%                (AMIGO_SData)==>> n_s{iexp}; t_s{iexp}; 
%                                  data_type; noise_type; std_dev{iexp}                            
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

nfkb_model                   % LOADS MODEL 
nfkb_experimental_scheme     % LOADS EXPERIMENTAL SCHEME

 
%==================================
% EXPERIMENTAL DATA RELATED INFO
%================================== 
 
inputs.exps.data_type='pseudo_pos';
inputs.exps.noise_type='homo_var';

%EXPERIMENT 1
 inputs.exps.std_dev{1}=ones(1,6).*0.10;

%EXPERIMENT 2
 inputs.exps.std_dev{2}=ones(1,6).*0.10;

