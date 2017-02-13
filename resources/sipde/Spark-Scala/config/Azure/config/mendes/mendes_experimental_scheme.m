% $Header$
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% mendes_experimental_scheme.m
%      
%
% EXPERIMENTAL SCHEME: 16 experiments performed under different S and P 
%                     (inputs) conditions and where all states are measured 
%                      at 21 equidistant sampling times
%
% REFERENCES:
%      >Moles, C. G., Pedro Mendes and Julio R. Banga (2003) Parameter
%       estimation in biochemical pathways: a comparison of global
%       optimization methods. Genome Research, 13(11):2467-2474 
%      >Rodriguez-Fernandez, M., J. A. Egea and J. R. Banga (2006) Novel
%       Metaheuristic for Parameter Estimation in Nonlinear Dynamic Biological 
%       Systems. BMC Bioinformatics 7:483.
%
% NOTE!!!: [] indicates that the corresponding input may be omitted,
%              default value will be assigned
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%==================================
% EXPERIMENTAL SCHEME RELATED DATA
%==================================

 inputs.exps.n_exp=16;                                % Number of experiments
 
 % Most inputs are common to all experiments therefore a loop over
 % experiments is defined
 for iexp=1:inputs.exps.n_exp
 inputs.exps.obs{iexp}='states';                      % All states in model are measured 
 inputs.exps.exp_y0{iexp}=[6.6667e-1 5.7254e-1 4.1758e-1...
 4.0e-1 3.6409e-1 2.9457e-1 1.419 9.3464e-1];   % Initial conditions for each experiment       
 inputs.exps.t_f{iexp}=120;                               % Experiments duration
 inputs.exps.n_s{iexp}=21;                                % Number of sampling times
  
 inputs.exps.u_interp{iexp}='sustained';               % [] Stimuli definition: u_interp: 'sustained' |'step'|'linear'(default)|
                                                       %                               'pulse-up'|'pulse-down'
 inputs.exps.t_con{iexp}= [0 120];
 end

 inputs.exps.u{1}=[0.1; 0.05];
 inputs.exps.u{2}=[0.1; 0.13572];
 inputs.exps.u{3}=[0.1; 0.3684];
 inputs.exps.u{4}=[0.1; 1];
 inputs.exps.u{5}=[0.46416; 0.05];
 inputs.exps.u{6}=[0.46416; 0.13572];
 inputs.exps.u{7}=[0.46416; 0.3684];
 inputs.exps.u{8}=[0.46416; 1];
 inputs.exps.u{9}=[2.1544; 0.05];
 inputs.exps.u{10}=[2.1544; 0.13572];
 inputs.exps.u{11}=[2.1544; 0.3684];
 inputs.exps.u{12}=[2.1544; 1];
 inputs.exps.u{13}=[10; 0.05];
 inputs.exps.u{14}=[10; 0.13572];
 inputs.exps.u{15}=[10; 0.3684];
 inputs.exps.u{16}=[10; 1];
 
 
